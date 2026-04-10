need rust to parse parquet files and insert into falkordb and i need to do it so fast that falkordb crashes, fails or crawls to a halt and then i need to optimize falkordb installation
please also the question what /dev/shm is?
my friend made a script that produces parquet files on /dev/shm/ubm-data/client{1-290}/{unixtimestamp}/*.parquet
now i need to parse all the parquet files and insert into falkor

I’m going to ground this in current FalkorDB and Rust docs, then give you a concrete ingestion design that can push FalkorDB hard without turning the loader itself into the bottleneck.

Tänkte i 24s

Here’s the practical way to think about this.

You have three separate problems:

Read a very large Parquet tree fast in Rust
Insert into FalkorDB fast enough to expose the database bottleneck
Tune FalkorDB so the database, not your loader, is the thing you improve

And /dev/shm is a fourth, small but important one.

What /dev/shm is

On Linux, /dev/shm is typically a tmpfs mount: a filesystem backed by RAM, not a normal disk. That makes it very fast for temporary data, but it is also volatile and disappears on reboot. In practice, it is commonly used for high-speed temporary files and shared memory between processes.

So your friend writing Parquet to:

/dev/shm/ubm-data/client{1-290}/{unixtimestamp}/*.parquet

means:

reads will usually be very fast,
you can overwhelm downstream systems easily,
but you should not treat the source files as durable storage.
Biggest design decision: do not start with naïve Cypher inserts

If your goal is to push FalkorDB hard, the slowest possible design is usually:

read one row,
build one CREATE query,
send one network round-trip,
repeat millions of times.

That mostly benchmarks your client and network.

FalkorDB exposes a GRAPH.BULK endpoint specifically for bulk import, and FalkorDB’s own docs say it is what their bulk loader uses to build a graph from binary-packed queries. They also note important limits: Redis has a 512 MB max string length and a default 1 GB max query size, so large imports must be incremental.

So the right progression is:

Phase A

Use batched Cypher via UNWIND to get something working quickly.

Phase B

If you need maximum throughput, move to GRAPH.BULK for initial graph creation.

That is how you make FalkorDB itself sweat.

Recommended ingestion architecture
1) Pipeline shape

Use a multi-stage Rust pipeline:

walk files -> read parquet -> convert to typed records -> batch by entity type -> writer workers -> FalkorDB

Use bounded channels between stages so you can control memory and backpressure.

A good structure is:

1 file discovery task
N parquet reader tasks
M transformer/batcher tasks
K DB writer tasks

Where:

N is usually tied to storage/CPU
K should start near FalkorDB THREAD_COUNT
all queues are bounded
2) Read Parquet as Arrow RecordBatches

The official Rust parquet crate is the Arrow project’s native Rust implementation, and its Arrow integration is intended to read/write RecordBatch values efficiently. The async reader module provides a stream of RecordBatches.

That means your unit of work should be a RecordBatch, not a single row.

3) Batch inserts aggressively

For FalkorDB, you want fewer, fatter writes.

Good options:

batch 5k to 50k rows per write for UNWIND
separate node batches from edge batches
keep property maps simple and typed
deduplicate labels/types per batch
4) Keep idempotency separate from throughput testing

If the goal is “make FalkorDB crawl,” avoid expensive merge-heavy logic in the first benchmark unless that’s your actual production workload.

For example:

fastest import test: CREATE
more realistic but slower: MERGE
best pure bootstrap path: GRAPH.BULK
Rust approach
Crates

You’ll likely want:

[dependencies]
tokio = { version = "1", features = ["full"] }
walkdir = "2"
futures = "0.3"
bytes = "1"
anyhow = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
parquet = { version = "58", features = ["arrow", "async"] }
arrow-array = "58"
arrow-schema = "58"
redis = { version = "0.32", features = ["tokio-comp", "connection-manager"] }

parquet 58.1.0 is the current official Rust crate page I found in Arrow’s docs.

Skeleton

This is the shape I would build first.

use anyhow::{Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use redis::AsyncCommands;
use std::path::{Path, PathBuf};
use tokio::{fs::File, sync::mpsc};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct ClientRow {
    client_id: i64,
    ts: i64,
    name: Option<String>,
    status: Option<String>,
}

#[derive(Debug)]
struct Batch {
    rows: Vec<ClientRow>,
}

fn discover_parquet_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
        .map(|e| e.path().to_path_buf())
        .collect()
}

async fn read_parquet_file(path: PathBuf) -> Result<Vec<ClientRow>> {
    let file = File::open(&path)
        .await
        .with_context(|| format!("open {}", path.display()))?;

    let builder = ParquetRecordBatchStreamBuilder::new(file).await
        .with_context(|| format!("parquet builder {}", path.display()))?;

    let mut stream = builder.build()?;
    let mut out = Vec::new();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let schema = batch.schema();

        let client_idx = schema.index_of("client_id")?;
        let ts_idx = schema.index_of("timestamp")?;
        let name_idx = schema.index_of("name").ok();
        let status_idx = schema.index_of("status").ok();

        let client_arr = batch
            .column(client_idx)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .context("client_id not Int64")?;

        let ts_arr = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .context("timestamp not Int64")?;

        let name_arr = name_idx.and_then(|i| {
            batch.column(i)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
        });

        let status_arr = status_idx.and_then(|i| {
            batch.column(i)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
        });

        for row in 0..batch.num_rows() {
            let client_id = client_arr.value(row);
            let ts = ts_arr.value(row);
            let name = name_arr.map(|a| if a.is_null(row) { None } else { Some(a.value(row).to_string()) }).flatten();
            let status = status_arr.map(|a| if a.is_null(row) { None } else { Some(a.value(row).to_string()) }).flatten();

            out.push(ClientRow {
                client_id,
                ts,
                name,
                status,
            });
        }
    }

    Ok(out)
}

fn build_unwind_query(rows: &[ClientRow]) -> Result<(String, String)> {
    let payload = serde_json::to_string(rows.iter().map(|r| {
        serde_json::json!({
            "client_id": r.client_id,
            "ts": r.ts,
            "name": r.name,
            "status": r.status
        })
    }).collect::<Vec<_>>())?;

    let query = r#"
UNWIND $rows AS row
CREATE (c:Client {
  client_id: row.client_id,
  ts: row.ts,
  name: row.name,
  status: row.status
})
"#.to_string();

    Ok((query, payload))
}

async fn writer_loop(
    mut rx: mpsc::Receiver<Batch>,
    redis_url: &str,
    graph_name: &str,
) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    while let Some(batch) = rx.recv().await {
        let (query, payload) = build_unwind_query(&batch.rows)?;
        let _: redis::Value = redis::cmd("GRAPH.QUERY")
            .arg(graph_name)
            .arg(query)
            .arg("--params")
            .arg(payload)
            .query_async(&mut conn)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from("/dev/shm/ubm-data");
    let files = discover_parquet_files(&root);

    let (tx, rx) = mpsc::channel::<Batch>(64);

    let writer = tokio::spawn(writer_loop(
        rx,
        "redis://:password@127.0.0.1:6379/",
        "ubm_graph",
    ));

    stream::iter(files)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(16, |path| {
            let tx = tx.clone();
            async move {
                let rows = read_parquet_file(path).await?;
                for chunk in rows.chunks(10_000) {
                    tx.send(Batch { rows: chunk.to_vec() }).await
                        .context("send batch")?;
                }
                Ok(())
            }
        })
        .await?;

    drop(tx);
    writer.await??;
    Ok(())
}
Important note about the code above

This is the fastest path to a working loader, not the fastest possible loader.

To really push FalkorDB:

avoid converting every field to owned String unless needed,
avoid materializing an entire file into Vec<ClientRow> if files are large,
batch directly from RecordBatch,
switch from JSON params to lower-overhead formats if client/library support allows,
and eventually move from GRAPH.QUERY to GRAPH.BULK for initial loads.
How to make FalkorDB, not Rust, become the bottleneck

If the test objective is “find where FalkorDB falls over,” do this in order.

Stage 1: Saturate with UNWIND

Run several concurrent writers, each sending batches like:

UNWIND $rows AS row
CREATE (:Client { ... })

Measure:

rows/sec
query latency p50/p95/p99
Redis/FalkorDB memory growth
CPU usage
queue backlog in your loader
failures/timeouts
Stage 2: Increase concurrency until behavior degrades

Increase:

DB writer count
rows per batch
total in-flight batches

You are looking for:

latency knee
timeout threshold
memory blow-up
server stalls
connection resets
queue saturation
Stage 3: Use GRAPH.BULK for initial graph loads

FalkorDB documents GRAPH.BULK as the endpoint their bulk loader uses, and explicitly says it is for building a new graph from binary data. It also says it cannot update existing graphs; the first BULK in a sequence uses "BEGIN".

That makes it a better fit than Cypher when your job is:

build a fresh graph from Parquet,
measure absolute ingest ceiling,
and then tune the DB installation.
FalkorDB tuning knobs that matter first

FalkorDB’s docs list several configuration parameters. The most relevant starting ones here are:

THREAD_COUNT
MAX_QUEUED_QUERIES
TIMEOUT
TIMEOUT_MAX
QUERY_MEM_CAPACITY
CACHE_SIZE
OMP_THREAD_COUNT
NODE_CREATION_BUFFER

The docs say THREAD_COUNT is the size of FalkorDB’s thread pool and effectively the maximum number of queries processed concurrently, and that it defaults to the machine’s logical cores.

Good first-pass Docker config

FalkorDB’s docs recommend the lighter falkordb/falkordb-server image for production use, not the browser image. They also show configuration through FALKORDB_ARGS and persistence through /data.

Example baseline:

services:
  falkordb:
    image: falkordb/falkordb-server:latest
    ports:
      - "6379:6379"
    environment:
      - REDIS_ARGS=--appendonly yes
      - FALKORDB_ARGS=THREAD_COUNT 8 TIMEOUT_MAX 30000 MAX_QUEUED_QUERIES 2048
    volumes:
      - falkordb_data:/data
    restart: unless-stopped
What to tune first
1) THREAD_COUNT

Start near physical cores or logical cores, then benchmark. More is not always better if contention goes up. FalkorDB says this controls its query-processing thread pool.

2) MAX_QUEUED_QUERIES

If writers get rejected too early, raise it. If latency becomes unbounded, lower it and rely on client backpressure instead. This is usually a stability knob, not a throughput knob.

3) TIMEOUT / TIMEOUT_MAX

Useful so pathological import queries fail fast instead of wedging the system forever. FalkorDB documents both.

4) QUERY_MEM_CAPACITY

If very large batches cause memory pressure, reduce batch size first, then tune this.

5) persistence settings

If you are chasing max raw ingest, benchmark with and without append-only persistence because durability settings can change throughput materially. FalkorDB documents Docker persistence setup using volumes and append-only mode.

Practical optimization strategy
Loader-side

Do these before blaming FalkorDB:

read Parquet in parallel
process in RecordBatch chunks
avoid per-row allocations
avoid tiny queries
use bounded queues
keep one multiplexed connection per writer, not one connection per row
measure serialization time separately from DB time
DB-side

Then tune FalkorDB:

use falkordb-server
pin CPU and memory cleanly
set THREAD_COUNT
test with persistence on and off
watch for queue saturation
prefer building a new graph with GRAPH.BULK for bootstrap imports
System-side

Since source data is in /dev/shm, your input side is likely already very fast. So likely bottlenecks become:

CPU spent decoding Parquet
client-side JSON/query construction
FalkorDB query execution
memory pressure in Redis/FalkorDB
persistence overhead
Important caution about “make it crash”

It is fine to do aggressive load testing on your own environment, but do it on:

a non-production FalkorDB instance,
with explicit memory and CPU limits,
with metrics enabled,
and with a known rollback path.

The useful goal is not “crash it randomly.” The useful goal is:

find throughput ceiling,
identify the first failing resource,
tune one variable at a time,
re-run,
document the knee point.

That gives you something reproducible.

My blunt recommendation

For your case, I would do this:

Version 1

Build a Rust loader that:

recursively scans /dev/shm/ubm-data
reads Parquet as Arrow RecordBatch
converts batches into node/edge chunks
writes with concurrent UNWIND queries
records rows/sec, errors, latency
Version 2

When that works, replace the write layer with a GRAPH.BULK implementation for fresh graph creation. That is the path most likely to expose FalkorDB’s real ingest ceiling.

Version 3

Tune FalkorDB:

falkordb/falkordb-server
adjust THREAD_COUNT
set queue/timeouts sanely
benchmark persistence modes
reduce client-side allocations

If you want, I can turn this into a complete Rust project with:

directory walker,
Parquet schema mapper,
concurrent batch writers,
and a Docker Compose file for a tuned FalkorDB test rig.
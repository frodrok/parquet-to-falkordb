use anyhow::{Context, Result};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use walkdir::WalkDir;

#[derive(Debug, Clone, serde::Serialize)]
struct GenericRow {
    source_file: String,
    row_index_in_batch: usize,
}

fn discover_parquet_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect()
}

async fn inspect_parquet_file(path: &Path) -> Result<()> {
    let file = File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;

    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .with_context(|| format!("failed to build parquet reader for {}", path.display()))?;

    let schema = builder.schema().clone();
    println!("\n=== FILE: {} ===", path.display());
    println!("Schema: {schema:?}");

    let mut stream = builder.build()?;
    let mut total_rows = 0usize;
    let mut batch_count = 0usize;

    while let Some(batch) = stream.next().await {
        let batch = batch.with_context(|| format!("error reading batch from {}", path.display()))?;
        batch_count += 1;
        total_rows += batch.num_rows();

        println!(
            "  batch {:>4}: rows={:>8}, cols={:>4}",
            batch_count,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    println!(
        "SUMMARY {}: batches={}, rows={}",
        path.display(),
        batch_count,
        total_rows
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/dev/shm/ubm-data".to_string());

    let root = PathBuf::from(root);

    if !root.exists() {
        anyhow::bail!("input root does not exist: {}", root.display());
    }

    let files = discover_parquet_files(&root);
    println!("Found {} parquet files under {}", files.len(), root.display());

    if files.is_empty() {
        return Ok(());
    }

    // Start conservatively. We will raise this later to stress FalkorDB.
    let concurrency = 8usize;

    let mut handles = Vec::new();
    for chunk in files.chunks(concurrency) {
        handles.clear();

        for path in chunk {
            let path = path.clone();
            handles.push(tokio::spawn(async move { inspect_parquet_file(&path).await }));
        }

        for handle in handles.drain(..) {
            handle.await??;
        }
    }

    Ok(())
}
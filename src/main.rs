use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, RecordBatch,
    StringArray,
};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use redis::aio::MultiplexedConnection;
use serde::Serialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize)]
struct CreatureRow {
    id: String,
    name: Option<String>,
    distrikt: Option<String>,
    kommun_id: Option<String>,
    fk_person_id: Option<String>,
    af_arende_id: Option<String>,
    region_patient_id: Option<String>,
    skv_personnr_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ServiceRow {
    id: String,
    creature_id: String,
    label: String,
    props: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct FormanRow {
    personnummer: String,
    forman_id: String,
    startdatum: i32,
    slutdatum: Option<i32>,
    totalbelopp: f64,
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

fn file_stem_str(path: &Path) -> Result<&str> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("bad file stem for {}", path.display()))
}

fn get_string_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} is not StringArray"))
}

fn get_binary_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BinaryArray> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("column {name} is not BinaryArray"))
}

fn get_f64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("column {name} is not Float64Array"))
}

fn get_i32_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("column {name} is not Int32Array"))
}

fn get_bool_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow!("column {name} is not BooleanArray"))
}

fn get_date32_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Date32Array> {
    let idx = batch.schema().index_of(name)?;
    batch.column(idx)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| anyhow!("column {name} is not Date32Array"))
}

fn opt_string(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let arr = batch.column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} is not StringArray"))?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(arr.value(row).to_string()))
    }
}

fn opt_f64(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<f64>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let arr = batch.column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("column {name} is not Float64Array"))?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(arr.value(row)))
    }
}

fn opt_i32(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i32>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let arr = batch.column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("column {name} is not Int32Array"))?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(arr.value(row)))
    }
}

fn opt_bool(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<bool>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let arr = batch.column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| anyhow!("column {name} is not BooleanArray"))?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(arr.value(row)))
    }
}

fn opt_date32(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i32>> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let arr = batch.column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| anyhow!("column {name} is not Date32Array"))?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(arr.value(row)))
    }
}

fn normalize_binary_id(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => hex::encode(bytes),
    }
}

async fn open_batch_stream(path: &Path) -> Result<parquet::arrow::async_reader::ParquetRecordBatchStream<File>> {
    let file = File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .with_context(|| format!("build stream {}", path.display()))?;
    Ok(builder.build()?)
}

async fn graph_query(
    conn: &mut MultiplexedConnection,
    graph_name: &str,
    query: &str,
    params: &serde_json::Value,
) -> Result<()> {
    let params_json = serde_json::to_string(params)?;
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg(graph_name)
        .arg(query)
        .arg("--params")
        .arg(params_json)
        .query_async(conn)
        .await
        .with_context(|| format!("GRAPH.QUERY failed: {query}"))?;
    Ok(())
}

async fn load_creatures(
    conn: &mut MultiplexedConnection,
    graph_name: &str,
    files: &[PathBuf],
) -> Result<()> {
    let creature_files: Vec<_> = files
        .iter()
        .filter(|p| file_stem_str(p).ok() == Some("creatures"))
        .cloned()
        .collect();

    println!("Loading creatures from {} files", creature_files.len());

    for path in creature_files {
        let mut stream = open_batch_stream(&path).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let ids = get_binary_col(&batch, "id")?;
            let names = get_string_col(&batch, "name")?;
            let distrikts = get_string_col(&batch, "distrikt")?;

            let mut rows = Vec::with_capacity(batch.num_rows());

            for row in 0..batch.num_rows() {
                let id = if ids.is_null(row) {
                    bail!("null creature id in {}", path.display());
                } else {
                    normalize_binary_id(ids.value(row))
                };

                let name = if names.is_null(row) {
                    None
                } else {
                    Some(names.value(row).to_string())
                };

                let distrikt = if distrikts.is_null(row) {
                    None
                } else {
                    Some(distrikts.value(row).to_string())
                };

                rows.push(CreatureRow {
                    id,
                    name,
                    distrikt,
                    kommun_id: opt_string(&batch, "kommun_id", row)?,
                    fk_person_id: opt_string(&batch, "fk_person_id", row)?,
                    af_arende_id: opt_string(&batch, "af_arende_id", row)?,
                    region_patient_id: opt_string(&batch, "region_patient_id", row)?,
                    skv_personnr_ref: opt_string(&batch, "skv_personnr_ref", row)?,
                });
            }

            let params = json!({ "rows": rows });
            let query = r#"
UNWIND $rows AS row
MERGE (c:Creature {id: row.id})
SET c.name = row.name,
    c.distrikt = row.distrikt,
    c.kommun_id = row.kommun_id,
    c.fk_person_id = row.fk_person_id,
    c.af_arende_id = row.af_arende_id,
    c.region_patient_id = row.region_patient_id,
    c.skv_personnr_ref = row.skv_personnr_ref
"#;

            graph_query(conn, graph_name, query, &params).await?;
            println!("Loaded {} creature rows from {}", batch.num_rows(), path.display());
        }
    }

    Ok(())
}

fn classify_label(stem: &str) -> Option<&'static str> {
    match stem {
        "personlig_assistans" => Some("PersonligAssistans"),
        "barn_unga" => Some("BarnUnga"),
        "boende_daglig_verksamhet" => Some("BoendeDagligVerksamhet"),
        "ekonomiskt_bistand" => Some("EkonomisktBistand"),
        "forsorjningsstod" => Some("Forsorjningsstod"),
        "hemtjanst" => Some("Hemtjanst"),
        "formaner" => Some("Forman"),
        "creatures" => None,
        _ => None,
    }
}

async fn load_service_file(
    conn: &mut MultiplexedConnection,
    graph_name: &str,
    path: &Path,
) -> Result<()> {
    let stem = file_stem_str(path)?;
    let label = match classify_label(stem) {
        Some(x) => x,
        None => return Ok(()),
    };

    if stem == "formaner" {
        let mut stream = open_batch_stream(path).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let personnummer = get_string_col(&batch, "personnummer")?;
            let forman_id = get_string_col(&batch, "förmån_id")?;
            let startdatum = get_date32_col(&batch, "startdatum")?;
            let totalbelopp = get_f64_col(&batch, "totalbelopp")?;

            let mut rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                rows.push(FormanRow {
                    personnummer: personnummer.value(row).to_string(),
                    forman_id: forman_id.value(row).to_string(),
                    startdatum: startdatum.value(row),
                    slutdatum: opt_date32(&batch, "slutdatum", row)?,
                    totalbelopp: totalbelopp.value(row),
                });
            }

            let params = json!({ "rows": rows });
            let query = r#"
UNWIND $rows AS row
MATCH (c:Creature {skv_personnr_ref: row.personnummer})
MERGE (f:Forman {id: row.forman_id})
SET f.personnummer = row.personnummer,
    f.startdatum = row.startdatum,
    f.slutdatum = row.slutdatum,
    f.totalbelopp = row.totalbelopp
MERGE (f)-[:FOR_CREATURE]->(c)
"#;
            graph_query(conn, graph_name, query, &params).await?;
            println!("Loaded {} formaner rows from {}", batch.num_rows(), path.display());
        }
        return Ok(());
    }

    let mut stream = open_batch_stream(path).await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let ids = get_string_col(&batch, "id")?;
        let creature_ids = get_string_col(&batch, "creature_id")?;

        let mut rows = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let id = ids.value(row).to_string();
            let creature_id = creature_ids.value(row).to_string();

            let props = match stem {
                "personlig_assistans" => json!({
                    "utforare_id": opt_string(&batch, "utforare_id", row)?,
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "lagrum": opt_string(&batch, "lagrum", row)?,
                    "beviljade_timmar_vecka": opt_f64(&batch, "beviljade_timmar_vecka", row)?,
                    "utforda_timmar_vecka": opt_f64(&batch, "utforda_timmar_vecka", row)?,
                    "finansiar": opt_string(&batch, "finansiar", row)?,
                    "ivo_tillsynsnummer": opt_string(&batch, "ivo_tillsynsnummer", row)?,
                    "beslutsdatum": opt_date32(&batch, "beslutsdatum", row)?,
                    "giltigt_till": opt_date32(&batch, "giltigt_till", row)?,
                    "status": opt_string(&batch, "status", row)?,
                }),
                "barn_unga" => json!({
                    "utforare_id": opt_string(&batch, "utforare_id", row)?,
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "insatstyp": opt_string(&batch, "insatstyp", row)?,
                    "lagrum": opt_string(&batch, "lagrum", row)?,
                    "placeringskommun": opt_string(&batch, "placeringskommun", row)?,
                    "kostnad_per_dygn": opt_f64(&batch, "kostnad_per_dygn", row)?,
                    "startdatum": opt_date32(&batch, "startdatum", row)?,
                    "slutdatum": opt_date32(&batch, "slutdatum", row)?,
                }),
                "boende_daglig_verksamhet" => json!({
                    "utforare_id": opt_string(&batch, "utforare_id", row)?,
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "typ": opt_string(&batch, "typ", row)?,
                    "lagrum": opt_string(&batch, "lagrum", row)?,
                    "besok_per_manad": opt_i32(&batch, "besok_per_manad", row)?,
                    "beslutsdatum": opt_date32(&batch, "beslutsdatum", row)?,
                    "giltighetsdatum": opt_date32(&batch, "giltighetsdatum", row)?,
                }),
                "ekonomiskt_bistand" => json!({
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "andamal": opt_string(&batch, "andamal", row)?,
                    "belopp_kr": opt_f64(&batch, "belopp_kr", row)?,
                    "period": opt_string(&batch, "period", row)?,
                    "aterkrav": opt_bool(&batch, "aterkrav", row)?,
                    "aterkravsbelopp": opt_f64(&batch, "aterkravsbelopp", row)?,
                    "utbetalningsdatum": opt_date32(&batch, "utbetalningsdatum", row)?,
                }),
                "forsorjningsstod" => json!({
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "belopp_kr": opt_f64(&batch, "belopp_kr", row)?,
                    "period": opt_string(&batch, "period", row)?,
                    "antal_manader": opt_i32(&batch, "antal_manader", row)?,
                    "aktivitetskrav_uppfyllt": opt_bool(&batch, "aktivitetskrav_uppfyllt", row)?,
                    "kopplad_aktivitet": opt_string(&batch, "kopplad_aktivitet", row)?,
                }),
                "hemtjanst" => json!({
                    "utforare_id": opt_string(&batch, "utforare_id", row)?,
                    "handlaggare_id": opt_string(&batch, "handlaggare_id", row)?,
                    "driftform": opt_string(&batch, "driftform", row)?,
                    "beviljade_timmar_manad": opt_f64(&batch, "beviljade_timmar_manad", row)?,
                    "utforda_timmar_manad": opt_f64(&batch, "utforda_timmar_manad", row)?,
                    "insatstyper": opt_string(&batch, "insatstyper", row)?,
                    "kostnad_kr": opt_f64(&batch, "kostnad_kr", row)?,
                    "beslutsdatum": opt_date32(&batch, "beslutsdatum", row)?,
                    "giltigt_till": opt_date32(&batch, "giltigt_till", row)?,
                }),
                _ => bail!("unhandled stem {stem}"),
            };

            rows.push(ServiceRow {
                id,
                creature_id,
                label: label.to_string(),
                props,
            });
        }

        // label cannot be parameterized, so use one fixed query per file type
        let query = format!(
            r#"
UNWIND $rows AS row
MATCH (c:Creature {{id: row.creature_id}})
MERGE (s:{label} {{id: row.id}})
SET s += row.props
MERGE (s)-[:FOR_CREATURE]->(c)
"#,
            label = label
        );

        let params = json!({ "rows": rows });
        graph_query(conn, graph_name, &query, &params).await?;
        println!("Loaded {} {} rows from {}", batch.num_rows(), stem, path.display());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/dev/shm/ubm-data".to_string());

    let redis_url = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "redis://127.0.0.1:6379/".to_string());

    let graph_name = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "ubm_graph".to_string());

    let root = PathBuf::from(root);
    if !root.exists() {
        bail!("input root does not exist: {}", root.display());
    }

    let files = discover_parquet_files(&root);
    println!("Found {} parquet files under {}", files.len(), root.display());

    let client = redis::Client::open(redis_url.clone())?;
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
        .with_context(|| format!("connect redis/falkordb at {redis_url}"))?;

    load_creatures(&mut conn, &graph_name, &files).await?;

    for path in &files {
        let stem = file_stem_str(path)?;
        if stem != "creatures" {
            if let Some(_) = classify_label(stem) {
                load_service_file(&mut conn, &graph_name, path).await?;
            }
        }
    }

    println!("Done loading graph {}", graph_name);
    Ok(())
}
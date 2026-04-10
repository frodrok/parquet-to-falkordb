use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, RecordBatch,
    StringArray,
};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

async fn open_batch_stream(
    path: &Path,
) -> Result<parquet::arrow::async_reader::ParquetRecordBatchStream<File>> {
    let file = File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .with_context(|| format!("build stream {}", path.display()))?;
    Ok(builder.build()?)
}

async fn graph_query_raw(
    conn: &mut redis::aio::MultiplexedConnection,
    graph_name: &str,
    query: &str,
) -> Result<()> {
    let mut cmd = redis::cmd("GRAPH.QUERY");
    cmd.arg(graph_name).arg(query);

    let fut = cmd.query_async::<redis::Value>(conn);

    timeout(Duration::from_secs(60), fut)
        .await
        .context("GRAPH.QUERY timed out after 60s")?
        .with_context(|| "GRAPH.QUERY failed")?;

    Ok(())
}

fn cypher_string(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("'{}'", escaped)
}

fn cypher_opt_string(v: &Option<String>) -> String {
    match v {
        Some(s) => cypher_string(s),
        None => "null".to_string(),
    }
}

fn cypher_opt_i32(v: Option<i32>) -> String {
    match v {
        Some(x) => x.to_string(),
        None => "null".to_string(),
    }
}

fn cypher_opt_f64(v: Option<f64>) -> String {
    match v {
        Some(x) if x.is_finite() => x.to_string(),
        _ => "null".to_string(),
    }
}

fn cypher_opt_bool(v: Option<bool>) -> String {
    match v {
        Some(true) => "true".to_string(),
        Some(false) => "false".to_string(),
        None => "null".to_string(),
    }
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

fn build_creature_query(rows: &[CreatureRow]) -> String {
    let list = rows
        .iter()
        .map(|r| {
            format!(
                "{{id:{},name:{},distrikt:{},kommun_id:{},fk_person_id:{},af_arende_id:{},region_patient_id:{},skv_personnr_ref:{}}}",
                cypher_string(&r.id),
                cypher_opt_string(&r.name),
                cypher_opt_string(&r.distrikt),
                cypher_opt_string(&r.kommun_id),
                cypher_opt_string(&r.fk_person_id),
                cypher_opt_string(&r.af_arende_id),
                cypher_opt_string(&r.region_patient_id),
                cypher_opt_string(&r.skv_personnr_ref),
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "UNWIND [{}] AS row
         CREATE (c:Creature {{
             id: row.id,
             name: row.name,
             distrikt: row.distrikt,
             kommun_id: row.kommun_id,
             fk_person_id: row.fk_person_id,
             af_arende_id: row.af_arende_id,
             region_patient_id: row.region_patient_id,
             skv_personnr_ref: row.skv_personnr_ref
         }})",
        list
    )
}

fn build_formaner_query(rows: &[FormanRow]) -> String {
    let list = rows
        .iter()
        .map(|r| {
            format!(
                "{{personnummer:{},forman_id:{},startdatum:{},slutdatum:{},totalbelopp:{}}}",
                cypher_string(&r.personnummer),
                cypher_string(&r.forman_id),
                r.startdatum,
                cypher_opt_i32(r.slutdatum),
                r.totalbelopp
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "UNWIND [{}] AS row
         MATCH (c:Creature {{skv_personnr_ref: row.personnummer}})
         CREATE (f:Forman {{
             id: row.forman_id,
             personnummer: row.personnummer,
             startdatum: row.startdatum,
             slutdatum: row.slutdatum,
             totalbelopp: row.totalbelopp
         }})
         CREATE (f)-[:FOR_CREATURE]->(c)",
        list
    )
}

fn build_personlig_assistans_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},utforare_id:{},handlaggare_id:{},lagrum:{},beviljade_timmar_vecka:{},utforda_timmar_vecka:{},finansiar:{},ivo_tillsynsnummer:{},beslutsdatum:{},giltigt_till:{},status:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "utforare_id", row)?),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_string(&opt_string(batch, "lagrum", row)?),
        cypher_opt_f64(opt_f64(batch, "beviljade_timmar_vecka", row)?),
        cypher_opt_f64(opt_f64(batch, "utforda_timmar_vecka", row)?),
        cypher_opt_string(&opt_string(batch, "finansiar", row)?),
        cypher_opt_string(&opt_string(batch, "ivo_tillsynsnummer", row)?),
        cypher_opt_i32(opt_date32(batch, "beslutsdatum", row)?),
        cypher_opt_i32(opt_date32(batch, "giltigt_till", row)?),
        cypher_opt_string(&opt_string(batch, "status", row)?),
    ))
}

fn build_barn_unga_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},utforare_id:{},handlaggare_id:{},insatstyp:{},lagrum:{},placeringskommun:{},kostnad_per_dygn:{},startdatum:{},slutdatum:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "utforare_id", row)?),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_string(&opt_string(batch, "insatstyp", row)?),
        cypher_opt_string(&opt_string(batch, "lagrum", row)?),
        cypher_opt_string(&opt_string(batch, "placeringskommun", row)?),
        cypher_opt_f64(opt_f64(batch, "kostnad_per_dygn", row)?),
        cypher_opt_i32(opt_date32(batch, "startdatum", row)?),
        cypher_opt_i32(opt_date32(batch, "slutdatum", row)?),
    ))
}

fn build_boende_daglig_verksamhet_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},utforare_id:{},handlaggare_id:{},typ:{},lagrum:{},besok_per_manad:{},beslutsdatum:{},giltighetsdatum:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "utforare_id", row)?),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_string(&opt_string(batch, "typ", row)?),
        cypher_opt_string(&opt_string(batch, "lagrum", row)?),
        cypher_opt_i32(opt_i32(batch, "besok_per_manad", row)?),
        cypher_opt_i32(opt_date32(batch, "beslutsdatum", row)?),
        cypher_opt_i32(opt_date32(batch, "giltighetsdatum", row)?),
    ))
}

fn build_ekonomiskt_bistand_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},handlaggare_id:{},andamal:{},belopp_kr:{},period:{},aterkrav:{},aterkravsbelopp:{},utbetalningsdatum:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_string(&opt_string(batch, "andamal", row)?),
        cypher_opt_f64(opt_f64(batch, "belopp_kr", row)?),
        cypher_opt_string(&opt_string(batch, "period", row)?),
        cypher_opt_bool(opt_bool(batch, "aterkrav", row)?),
        cypher_opt_f64(opt_f64(batch, "aterkravsbelopp", row)?),
        cypher_opt_i32(opt_date32(batch, "utbetalningsdatum", row)?),
    ))
}

fn build_forsorjningsstod_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},handlaggare_id:{},belopp_kr:{},period:{},antal_manader:{},aktivitetskrav_uppfyllt:{},kopplad_aktivitet:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_f64(opt_f64(batch, "belopp_kr", row)?),
        cypher_opt_string(&opt_string(batch, "period", row)?),
        cypher_opt_i32(opt_i32(batch, "antal_manader", row)?),
        cypher_opt_bool(opt_bool(batch, "aktivitetskrav_uppfyllt", row)?),
        cypher_opt_string(&opt_string(batch, "kopplad_aktivitet", row)?),
    ))
}

fn build_hemtjanst_row(batch: &RecordBatch, row: usize) -> Result<String> {
    Ok(format!(
        "{{id:{},creature_id:{},utforare_id:{},handlaggare_id:{},driftform:{},beviljade_timmar_manad:{},utforda_timmar_manad:{},insatstyper:{},kostnad_kr:{},beslutsdatum:{},giltigt_till:{}}}",
        cypher_string(&opt_string(batch, "id", row)?.unwrap()),
        cypher_string(&opt_string(batch, "creature_id", row)?.unwrap()),
        cypher_opt_string(&opt_string(batch, "utforare_id", row)?),
        cypher_opt_string(&opt_string(batch, "handlaggare_id", row)?),
        cypher_opt_string(&opt_string(batch, "driftform", row)?),
        cypher_opt_f64(opt_f64(batch, "beviljade_timmar_manad", row)?),
        cypher_opt_f64(opt_f64(batch, "utforda_timmar_manad", row)?),
        cypher_opt_string(&opt_string(batch, "insatstyper", row)?),
        cypher_opt_f64(opt_f64(batch, "kostnad_kr", row)?),
        cypher_opt_i32(opt_date32(batch, "beslutsdatum", row)?),
        cypher_opt_i32(opt_date32(batch, "giltigt_till", row)?),
    ))
}

fn build_service_query(label: &str, row_literals: &[String]) -> String {
    let list = row_literals.join(",");

    match label {
        "PersonligAssistans" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:PersonligAssistans {{
                 id: row.id,
                 utforare_id: row.utforare_id,
                 handlaggare_id: row.handlaggare_id,
                 lagrum: row.lagrum,
                 beviljade_timmar_vecka: row.beviljade_timmar_vecka,
                 utforda_timmar_vecka: row.utforda_timmar_vecka,
                 finansiar: row.finansiar,
                 ivo_tillsynsnummer: row.ivo_tillsynsnummer,
                 beslutsdatum: row.beslutsdatum,
                 giltigt_till: row.giltigt_till,
                 status: row.status
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        "BarnUnga" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:BarnUnga {{
                 id: row.id,
                 utforare_id: row.utforare_id,
                 handlaggare_id: row.handlaggare_id,
                 insatstyp: row.insatstyp,
                 lagrum: row.lagrum,
                 placeringskommun: row.placeringskommun,
                 kostnad_per_dygn: row.kostnad_per_dygn,
                 startdatum: row.startdatum,
                 slutdatum: row.slutdatum
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        "BoendeDagligVerksamhet" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:BoendeDagligVerksamhet {{
                 id: row.id,
                 utforare_id: row.utforare_id,
                 handlaggare_id: row.handlaggare_id,
                 typ: row.typ,
                 lagrum: row.lagrum,
                 besok_per_manad: row.besok_per_manad,
                 beslutsdatum: row.beslutsdatum,
                 giltighetsdatum: row.giltighetsdatum
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        "EkonomisktBistand" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:EkonomisktBistand {{
                 id: row.id,
                 handlaggare_id: row.handlaggare_id,
                 andamal: row.andamal,
                 belopp_kr: row.belopp_kr,
                 period: row.period,
                 aterkrav: row.aterkrav,
                 aterkravsbelopp: row.aterkravsbelopp,
                 utbetalningsdatum: row.utbetalningsdatum
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        "Forsorjningsstod" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:Forsorjningsstod {{
                 id: row.id,
                 handlaggare_id: row.handlaggare_id,
                 belopp_kr: row.belopp_kr,
                 period: row.period,
                 antal_manader: row.antal_manader,
                 aktivitetskrav_uppfyllt: row.aktivitetskrav_uppfyllt,
                 kopplad_aktivitet: row.kopplad_aktivitet
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        "Hemtjanst" => format!(
            "UNWIND [{}] AS row
             MATCH (c:Creature {{id: row.creature_id}})
             CREATE (s:Hemtjanst {{
                 id: row.id,
                 utforare_id: row.utforare_id,
                 handlaggare_id: row.handlaggare_id,
                 driftform: row.driftform,
                 beviljade_timmar_manad: row.beviljade_timmar_manad,
                 utforda_timmar_manad: row.utforda_timmar_manad,
                 insatstyper: row.insatstyper,
                 kostnad_kr: row.kostnad_kr,
                 beslutsdatum: row.beslutsdatum,
                 giltigt_till: row.giltigt_till
             }})
             CREATE (s)-[:FOR_CREATURE]->(c)",
            list
        ),
        _ => unreachable!("unknown label"),
    }
}

async fn load_creatures(
    conn: &mut redis::aio::MultiplexedConnection,
    graph_name: &str,
    files: &[PathBuf],
    batch_size: usize,
) -> Result<()> {
    let creature_files: Vec<_> = files
        .iter()
        .filter(|p| file_stem_str(p).ok() == Some("creatures"))
        .cloned()
        .collect();

    println!("Loading creatures from {} files", creature_files.len());

    let mut grand_total = 0usize;

    for path in creature_files {
        let mut stream = open_batch_stream(&path).await?;
        let mut total_rows = 0usize;

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

            total_rows += rows.len();
            grand_total += rows.len();

            for chunk in rows.chunks(batch_size) {
                let query = build_creature_query(chunk);
                graph_query_raw(conn, graph_name, &query).await?;
            }
        }

        println!("Loaded creatures file {} rows={}", path.display(), total_rows);
    }

    println!("Finished loading creatures total_rows={}", grand_total);
    Ok(())
}

async fn load_formaner_file(
    conn: &mut redis::aio::MultiplexedConnection,
    graph_name: &str,
    path: &Path,
    batch_size: usize,
) -> Result<usize> {
    let mut stream = open_batch_stream(path).await?;
    let mut total_rows = 0usize;

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

        total_rows += rows.len();

        for chunk in rows.chunks(batch_size) {
            let query = build_formaner_query(chunk);
            graph_query_raw(conn, graph_name, &query).await?;
        }
    }

    Ok(total_rows)
}

async fn load_service_file(
    conn: &mut redis::aio::MultiplexedConnection,
    graph_name: &str,
    path: &Path,
    batch_size: usize,
) -> Result<usize> {
    let stem = file_stem_str(path)?;
    let label = match classify_label(stem) {
        Some(x) => x,
        None => return Ok(0),
    };

    if stem == "formaner" {
        return load_formaner_file(conn, graph_name, path, batch_size).await;
    }

    let mut stream = open_batch_stream(path).await?;
    let mut total_rows = 0usize;

    while let Some(batch) = stream.next().await {
        let batch = batch?;

        let mut row_literals = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let lit = match stem {
                "personlig_assistans" => build_personlig_assistans_row(&batch, row)?,
                "barn_unga" => build_barn_unga_row(&batch, row)?,
                "boende_daglig_verksamhet" => build_boende_daglig_verksamhet_row(&batch, row)?,
                "ekonomiskt_bistand" => build_ekonomiskt_bistand_row(&batch, row)?,
                "forsorjningsstod" => build_forsorjningsstod_row(&batch, row)?,
                "hemtjanst" => build_hemtjanst_row(&batch, row)?,
                _ => bail!("unhandled stem {stem}"),
            };
            row_literals.push(lit);
        }

        total_rows += row_literals.len();

        for chunk in row_literals.chunks(batch_size) {
            let query = build_service_query(label, chunk);
            graph_query_raw(conn, graph_name, &query).await?;
        }
    }

    Ok(total_rows)
}

async fn worker_loop(
    worker_id: usize,
    redis_url: String,
    graph_name: String,
    batch_size: usize,
    mut rx: mpsc::Receiver<PathBuf>,
) -> Result<()> {
    let client = redis::Client::open(redis_url.clone())?;
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
        .with_context(|| format!("worker {worker_id} connect redis/falkordb at {redis_url}"))?;

    let mut files_done = 0usize;
    let mut rows_done = 0usize;

    while let Some(path) = rx.recv().await {
        let rows = load_service_file(&mut conn, &graph_name, &path, batch_size).await?;
        files_done += 1;
        rows_done += rows;

        println!(
            "[worker {}] loaded {} rows from {} (files_done={}, rows_done={})",
            worker_id,
            rows,
            path.display(),
            files_done,
            rows_done
        );
    }

    println!(
        "[worker {}] finished files_done={} rows_done={}",
        worker_id, files_done, rows_done
    );

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

    let batch_size = std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(500);

    let workers = std::env::var("WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8);

    let root = PathBuf::from(root);
    if !root.exists() {
        bail!("input root does not exist: {}", root.display());
    }

    let files = discover_parquet_files(&root);
    let unique: HashSet<_> = files.iter().collect();

    println!("Found {} parquet files under {}", files.len(), root.display());
    println!("Unique paths: {}", unique.len());
    println!("Using redis/falkordb URL: {}", redis_url);
    println!("Graph name: {}", graph_name);
    println!("Batch size: {}", batch_size);
    println!("Workers: {}", workers);

    {
        let client = redis::Client::open(redis_url.clone())?;
        let mut conn = client
            .get_multiplexed_tokio_connection()
            .await
            .with_context(|| format!("connect redis/falkordb at {redis_url}"))?;

        println!("Loading creatures phase...");
        load_creatures(&mut conn, &graph_name, &files, batch_size).await?;
    }

    let service_files: Vec<PathBuf> = files
        .into_iter()
        .filter(|path| match file_stem_str(path) {
            Ok(stem) => stem != "creatures" && classify_label(stem).is_some(),
            Err(_) => false,
        })
        .collect();

    println!("Loading service files phase... files={}", service_files.len());

    let mut senders = Vec::new();
    let mut handles = Vec::new();

    for worker_id in 0..workers {
        let (tx, rx) = mpsc::channel::<PathBuf>(32);
        senders.push(tx);

        let redis_url = redis_url.clone();
        let graph_name = graph_name.clone();

        handles.push(tokio::spawn(async move {
            worker_loop(worker_id + 1, redis_url, graph_name, batch_size, rx).await
        }));
    }

    for (idx, path) in service_files.into_iter().enumerate() {
        let worker_idx = idx % workers;
        senders[worker_idx]
            .send(path)
            .await
            .context("send path to worker")?;
    }

    drop(senders);

    for handle in handles {
        handle.await??;
    }

    println!("Done loading graph {}", graph_name);
    Ok(())
}
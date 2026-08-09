use std::fs;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use hestan::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DOCS_DIR: &str = "docs";
const MARKER: &str = "ingest.marker";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// name, byte length, mtime millis
type FileMeta = (String, u64, u128);

fn dir_entries(dir: &str) -> Result<Vec<FileMeta>, Box<dyn std::error::Error + Send + Sync>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if !meta.is_file() {
            continue;
        }
        let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_millis();
        entries.push((
            entry.file_name().to_string_lossy().into_owned(),
            meta.len(),
            mtime,
        ));
    }
    entries.sort();
    Ok(entries)
}

// content is deliberately not read: the probe must stay cheap
fn docs_fingerprint() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut hasher = Sha256::new();
    for (name, len, mtime) in dir_entries(DOCS_DIR)? {
        hasher.update(name.as_bytes());
        hasher.update(len.to_le_bytes());
        hasher.update(mtime.to_le_bytes());
    }
    Ok(hex(&hasher.finalize()))
}

#[derive(Debug, Serialize, Deserialize)]
struct DocStat {
    file: String,
    bytes: u64,
    lines: usize,
}

#[derive(Deserialize)]
struct TotalsIn {
    doc_stats: Vec<DocStat>,
}

#[derive(Serialize)]
struct Totals {
    files: usize,
    bytes: u64,
    lines: usize,
    largest: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    tracing_subscriber::fmt().init();

    let docs_dir = Asset::source("docs_dir")
        .probe(|| async { docs_fingerprint() })
        .probe_every(Duration::from_secs(10));

    // the source dep is lineage: its input is null, so this reads docs/ itself
    let doc_stats = Asset::new("doc_stats", |ctx| async move {
        let mut stats = Vec::new();
        for (file, bytes, _) in dir_entries(DOCS_DIR)? {
            let text = fs::read_to_string(Path::new(DOCS_DIR).join(&file))?;
            stats.push(DocStat {
                file,
                bytes,
                lines: text.lines().count(),
            });
        }
        ctx.info(format!("measured {} files", stats.len()));
        ctx.meta("files", stats.len() as i64);
        ctx.meta("bytes", stats.iter().map(|s| s.bytes).sum::<u64>() as i64);
        Ok(serde_json::to_value(stats)?)
    })
    .from(&docs_dir);

    let doc_totals = Asset::typed("doc_totals", |ctx: OpCtx, input: TotalsIn| async move {
        let stats = input.doc_stats;
        ctx.meta(
            "source",
            Meta::Url("https://github.com/quanalyst/hestan".into()),
        );
        Ok(Totals {
            files: stats.len(),
            bytes: stats.iter().map(|s| s.bytes).sum(),
            lines: stats.iter().map(|s| s.lines).sum(),
            largest: stats.iter().max_by_key(|s| s.bytes).map(|s| s.file.clone()),
        })
    })
    .from(&doc_stats)
    .auto();

    // every doc has content, and there are enough of them to be a doc set. the
    // first fails the run when it breaks; the second only says so.
    let non_empty = AssetCheck::new(
        "no_empty_docs",
        "doc_stats",
        |_ctx, value: Value| async move {
            let stats: Vec<DocStat> = serde_json::from_value(value)?;
            let empty: Vec<&str> = stats
                .iter()
                .filter(|s| s.lines == 0)
                .map(|s| s.file.as_str())
                .collect();
            if empty.is_empty() {
                Ok(CheckResult::pass().meta("checked", stats.len() as i64))
            } else {
                Ok(CheckResult::fail(format!("empty: {}", empty.join(", "))))
            }
        },
    );

    let enough_docs = AssetCheck::new(
        "enough_docs",
        "doc_totals",
        |_ctx, value: Value| async move {
            let files = value["files"].as_u64().unwrap_or(0);
            if files >= 10 {
                Ok(CheckResult::pass().meta("files", files as i64))
            } else {
                Ok(CheckResult::fail(format!("only {files} docs")).meta("files", files as i64))
            }
        },
    )
    .severity(Severity::Warn);

    let ingest = Job::builder("ingest_marker")
        .description("ingest the marker file's real content")
        .op(Op::new("read_marker", |ctx| async move {
            let text = fs::read_to_string(MARKER)?;
            ctx.info(format!("marker is {} bytes", text.len()));
            Ok(json!({
                "bytes": text.len(),
                "lines": text.lines().count(),
                "sha256": hex(&Sha256::digest(text.as_bytes())),
            }))
        }))
        .build()?;

    // the committed cursor is the last mtime seen, so nothing double-fires
    let marker_watch = Sensor::new("marker_file", Duration::from_secs(5), |ctx| async move {
        let Ok(meta) = fs::metadata(MARKER) else {
            return Ok(Vec::new());
        };
        let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_millis() as u64;
        if ctx.cursor_as::<u64>()? == Some(mtime) {
            return Ok(Vec::new());
        }
        ctx.set_cursor(json!(mtime));
        Ok(vec![RunRequest {
            job: "ingest_marker".into(),
            params: json!({ "path": MARKER }),
        }])
    });

    println!("hestan assets example: http://127.0.0.1:4002");
    Hestan::new()
        .assets([docs_dir, doc_stats, doc_totals])
        .check(non_empty)
        .check(enough_docs)
        .job(ingest)
        .sensor(marker_watch)
        .db("assets_demo.db")
        .serve(([127, 0, 0, 1], 4002))
        .await
}

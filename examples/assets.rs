use std::fs;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    // stderr and RUST_LOG, for the reason the demo says: this binary has a
    // command line, its stdout belongs to the answer, and hestan's own events
    // are what `--wait` is already streaming
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

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
        ctx.meta("files", Meta::count(stats.len() as u64));
        ctx.meta("bytes", Meta::bytes(stats.iter().map(|s| s.bytes).sum()));
        ctx.meta("dir", Meta::path(DOCS_DIR));
        // the five longest, which is the sample worth reading in the ui
        let mut longest = stats.clone();
        longest.sort_by_key(|s| std::cmp::Reverse(s.lines));
        ctx.meta(
            "longest",
            Meta::table(
                [("file", "text"), ("lines", "int"), ("bytes", "int")],
                longest
                    .iter()
                    .take(5)
                    .map(|s| vec![json!(s.file), json!(s.lines), json!(s.bytes)]),
            ),
        );
        Ok(serde_json::to_value(stats)?)
    })
    .from(&docs_dir);

    let doc_totals = Asset::typed("doc_totals", |ctx: OpCtx, input: TotalsIn| async move {
        let stats = input.doc_stats;
        ctx.meta(
            "source",
            Meta::Url("https://github.com/quanalyst/hestan".into()),
        );
        // what these totals are of: the ui links it, because hestan knows the
        // graph and does not need a url to point inside itself
        ctx.meta("measured", Meta::asset_ref("doc_stats"));
        ctx.meta("bytes", Meta::bytes(stats.iter().map(|s| s.bytes).sum()));
        Ok(Totals {
            files: stats.len(),
            bytes: stats.iter().map(|s| s.bytes).sum(),
            lines: stats.iter().map(|s| s.lines).sum(),
            largest: stats.iter().max_by_key(|s| s.bytes).map(|s| s.file.clone()),
        })
    })
    .from(&doc_stats)
    .auto();

    // one op, two assets: splitting what was measured once rather than
    // measuring it twice
    let split_docs = MultiAsset::new("split_docs", |ctx: OpCtx| async move {
        let value = ctx.input("doc_stats").cloned().unwrap_or(json!([]));
        let stats: Vec<DocStat> = serde_json::from_value(value)?;
        let (long, short): (Vec<DocStat>, Vec<DocStat>) =
            stats.into_iter().partition(|s| s.lines >= 100);
        ctx.meta_of("long_docs", "files", long.len() as i64);
        ctx.meta_of("short_docs", "files", short.len() as i64);
        Ok(json!({ "long_docs": long, "short_docs": short }))
    })
    .produces(["long_docs", "short_docs"])
    .from(&doc_stats);

    // one materialization per utc day: which docs changed that day. the last
    // ten days, so the partition grid has something to draw
    let start = (Utc::now() - chrono::Duration::days(9))
        .format("%Y-%m-%d")
        .to_string();
    let daily_changes = Asset::new("daily_doc_changes", |ctx: OpCtx| async move {
        let day = ctx.partition().expect("partitioned").to_string();
        let mut changed = Vec::new();
        for (file, bytes, mtime) in dir_entries(DOCS_DIR)? {
            let at = DateTime::<Utc>::from_timestamp_millis(mtime as i64)
                .ok_or("a file mtime outside the representable range")?;
            if at.format("%Y-%m-%d").to_string() == day {
                changed.push(json!({ "file": file, "bytes": bytes }));
            }
        }
        ctx.meta("files", changed.len() as i64);
        Ok(json!({ "day": day, "files": changed }))
    })
    .from(&docs_dir)
    .partitioned(Partitions::daily(start).build_limit(10));

    // one materialization per utc hour: how much of the docs directory was
    // written in it. two days of them, so a daily key has 24 to cover
    let start_hour = (Utc::now() - chrono::Duration::days(1))
        .format("%Y-%m-%dT00")
        .to_string();
    let hourly_writes = Asset::new("hourly_doc_writes", |ctx: OpCtx| async move {
        let hour = ctx.partition().expect("partitioned").to_string();
        let mut bytes = 0;
        for (_, len, mtime) in dir_entries(DOCS_DIR)? {
            let at = DateTime::<Utc>::from_timestamp_millis(mtime as i64)
                .ok_or("a file mtime outside the representable range")?;
            if at.format("%Y-%m-%dT%H").to_string() == hour {
                bytes += len;
            }
        }
        ctx.meta("bytes", bytes as i64);
        Ok(json!({ "hour": hour, "bytes": bytes }))
    })
    .from(&docs_dir)
    .partitioned(Partitions::hourly(start_hour).build_limit(48));

    // and the rollup this phase exists for: one daily key reading the 24 hourly
    // keys inside it. today's key covers hours that have not happened yet: it
    // rolls up the ones that have, and goes stale as each next one lands
    let start_day = (Utc::now() - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let daily_writes = Asset::new("daily_doc_writes", |ctx: OpCtx| async move {
        let hours = ctx.input("hourly_doc_writes").cloned().unwrap_or(json!({}));
        let hours = hours.as_object().cloned().unwrap_or_default();
        let bytes: u64 = hours
            .values()
            .map(|h| h["bytes"].as_u64().unwrap_or(0))
            .sum();
        ctx.meta("hours", hours.len() as i64);
        Ok(json!({ "day": ctx.partition(), "hours": hours.len(), "bytes": bytes }))
    })
    .reads(&hourly_writes, PartitionMapping::covering())
    .partitioned(Partitions::daily(start_day).build_limit(2));

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
        // the cursor already stops a second launch for the same mtime; the run
        // key is what stops one when a replay gets past the cursor
        Ok(vec![
            RunRequest::new("ingest_marker")
                .params(json!({ "path": MARKER }))
                .key(mtime.to_string()),
        ])
    });

    let app = Hestan::new()
        .assets([
            docs_dir,
            doc_stats,
            doc_totals,
            daily_changes,
            hourly_writes,
            daily_writes,
        ])
        .multi_assets([split_docs])
        .check(non_empty)
        .check(enough_docs)
        .job(ingest)
        .sensor(marker_watch)
        .db(std::env::var("HESTAN_DB").unwrap_or_else(|_| "assets_demo.db".into()));

    // the same mount the demo uses: with no arguments this serves on :4002 and
    // the example is what it was, and with any it is a command line over the
    // assets declared above: `assets`, `build`, `backfill` and the rest
    hestan::cli::run(app, ([127, 0, 0, 1], 4002)).await
}

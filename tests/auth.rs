//! a token, and every place it must not be.
//!
//! `harness = false` because this binary has to be both halves of it: a child
//! process serving an authenticated deployment with every tracing line on its
//! stdout, and a parent driving that deployment over http and then reading
//! everything the child wrote, everything it answered, and every byte of the
//! database it wrote it to.
//!
//! the point is the grep at the end. a credential that is checked correctly
//! and then written into a log line, an event payload or an error message is a
//! credential in a log aggregator, and hestan cannot take it back out.

use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{Auth, EventQuery, Store};

/// the token the deployment under test is configured with.
///
/// deliberately a string nothing else would produce: what the grep at the end
/// is worth depends entirely on a match being a leak rather than a
/// coincidence.
const TOKEN: &str = "tk-9f2c41d7e6b04a1c-not-in-any-output";

/// and one that is not it, presented on purpose: a 401 that echoes what it
/// refused is the same leak from the other direction.
const WRONG: &str = "tk-0000000000000000-refused-on-purpose";

/// where the child finds its run log. absent means "run the cases".
const DB: &str = "HESTAN_AUTH_DB";
/// what the child serves on.
const ADDR: &str = "HESTAN_AUTH_ADDR";

fn main() {
    if let Ok(db) = std::env::var(DB) {
        serve(&db);
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    a_credential_reaches_no_stream_no_row_and_no_response(dir.path());
    println!("auth: every case passed");
}

/// the deployment under test: authenticated, with everything the parent drives
/// compiled into it, and with every line it says on stdout where the parent can
/// read it.
fn serve(db: &str) {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(std::io::stdout)
        .init();
    let addr: SocketAddr = std::env::var(ADDR).unwrap().parse().unwrap();
    let app = Hestan::new()
        .job(
            Job::builder("quick")
                .op(Op::new("greet", |ctx: OpCtx| async move {
                    ctx.info("hello from quick");
                    Ok(json!({ "ok": true }))
                }))
                .build()
                .unwrap(),
        )
        .schedule("quick", "0 9 * * *")
        .db(db)
        .auth(Auth::bearer(TOKEN));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Err(e) = rt.block_on(app.serve(addr)) {
        eprintln!("serving failed: {e}");
        std::process::exit(70);
    }
}

/// drive an authenticated deployment through the things a person does, then
/// look everywhere the token could have ended up.
fn a_credential_reaches_no_stream_no_row_and_no_response(dir: &Path) {
    let db = dir.join("auth.db");
    let db = db.display().to_string();
    let addr = free_port();
    let child = Command::new(exe())
        .env(DB, &db)
        .env(ADDR, addr.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the deployment starts");

    // everything the deployment said back, kept for the grep: a response body
    // is an output stream too, and the one an attacker can read most easily
    let mut answers = String::new();
    let mut say = |answer: &Answer| answers.push_str(&format!("{answer:?}\n"));

    wait_for("the ui to come up", || {
        get(addr, "/api/whoami", None).ok().filter(|a| a.ok())
    });

    // an authenticated deployment says it is one, to anybody, because the ui
    // and the command line have to be able to ask before they hold anything
    let asked = get(addr, "/api/whoami", None).unwrap();
    assert!(asked.body.contains("\"auth\":true"), "{asked:?}");
    assert!(asked.body.contains("\"identity\":null"), "{asked:?}");
    say(&asked);

    // a stranger, and somebody with the wrong token
    for token in [None, Some(WRONG)] {
        let refused = get(addr, "/api/runs", token).unwrap();
        assert_eq!(refused.status, 401, "{refused:?}");
        say(&refused);
    }

    // and the things the token is for: read, launch, wait, retry, cancel,
    // pause — the whole of what an operator does in an afternoon
    let token = Some(TOKEN);
    let known = get(addr, "/api/whoami", token).unwrap();
    assert!(known.body.contains("\"role\":\"admin\""), "{known:?}");
    say(&known);
    say(&get(addr, "/api/health", token).unwrap());
    say(&get(addr, "/api/jobs", token).unwrap());

    let launched = post(addr, "/api/jobs/quick/runs", token, "{}").unwrap();
    assert_eq!(launched.status, 202, "{launched:?}");
    say(&launched);
    let run_id = field(&launched.body, "run_id");
    wait_for("the run to finish", || {
        let run = get(addr, &format!("/api/runs/{run_id}"), token).ok()?;
        run.body.contains("\"status\":\"success\"").then_some(run)
    });

    let retried = post(addr, &format!("/api/runs/{run_id}/retry"), token, "{}").unwrap();
    assert_eq!(retried.status, 202, "{retried:?}");
    say(&retried);
    say(&post(
        addr,
        &format!("/api/runs/{}/cancel", field(&retried.body, "run_id")),
        token,
        "{}",
    )
    .unwrap());
    let paused = post(
        addr,
        "/api/schedules/state",
        token,
        r#"{"job":"quick","expr":"0 9 * * *","paused":true}"#,
    )
    .unwrap();
    assert_eq!(paused.status, 200, "{paused:?}");
    say(&paused);
    say(&get(addr, "/api/events?limit=200", token).unwrap());
    say(&get(addr, &format!("/api/runs/{run_id}/events"), token).unwrap());

    let (out, err) = stop(child);
    let out = format!("{out}\n{err}");
    // the deployment did say things — a grep over an empty string passes
    assert!(out.contains("hestan"), "the child said nothing: {out}");
    assert!(out.contains("run queued"), "no run in the log: {out}");

    for (what, text) in [
        ("stdout and stderr", &out),
        ("the responses it sent", &answers),
    ] {
        for secret in [TOKEN, WRONG] {
            assert!(
                !text.contains(secret),
                "a credential reached {what}:\n{}",
                context(text, secret)
            );
        }
    }

    // the whole database, as bytes: not the columns somebody thought to check,
    // and not a query that a future column could hide behind
    let bytes = std::fs::read(&db).expect("the run log is a file");
    for secret in [TOKEN, WRONG] {
        assert!(
            !find(&bytes, secret.as_bytes()),
            "a credential is in the database file"
        );
    }
    // and the rows themselves, read back the way anything else would read
    // them, since a page of the file could be anywhere
    let store = Store::open(&db).unwrap();
    let events = store.event_log(&EventQuery::default(), 500).unwrap();
    assert!(!events.is_empty(), "no events were written");
    for event in &events {
        let row = format!("{event:?}");
        for secret in [TOKEN, WRONG] {
            assert!(!row.contains(secret), "a credential is on an event: {row}");
        }
    }
    for run in store.runs(None, None, None, None, None, 100).unwrap() {
        let row = format!("{run:?}");
        for secret in [TOKEN, WRONG] {
            assert!(!row.contains(secret), "a credential is on a run: {row}");
        }
    }
    // last: the configuration that holds it. a `Debug` on the way past is how
    // most credentials reach a log
    let printed = format!("{:?}", Auth::bearer(TOKEN));
    assert!(!printed.contains(TOKEN), "{printed}");

    println!("auth: a_credential_reaches_no_stream_no_row_and_no_response ok");
}

// ------------------------------------------------------------------ plumbing

/// one answer, whole: the status, the headers and the body, because the grep
/// at the end is over all three.
struct Answer {
    status: u16,
    headers: String,
    body: String,
}

impl Answer {
    fn ok(&self) -> bool {
        self.status == 200
    }
}

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}\n{}", self.status, self.headers, self.body)
    }
}

fn get(addr: SocketAddr, path: &str, token: Option<&str>) -> std::io::Result<Answer> {
    send(addr, "GET", path, token, None)
}

fn post(addr: SocketAddr, path: &str, token: Option<&str>, body: &str) -> std::io::Result<Answer> {
    send(addr, "POST", path, token, Some(body))
}

/// http/1.1 by hand, one request per connection.
///
/// hestan's own client is behind a feature this test does not need, and what
/// is being exercised here is one header and one body.
fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> std::io::Result<Answer> {
    let mut socket = TcpStream::connect(addr)?;
    let mut request =
        format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    match body {
        Some(body) => request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )),
        None => request.push_str("\r\n"),
    }
    socket.write_all(request.as_bytes())?;
    let mut said = String::new();
    socket.read_to_string(&mut said)?;
    let (head, body) = said.split_once("\r\n\r\n").unwrap_or((&said, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(Answer {
        status,
        headers: head.to_string(),
        body: body.to_string(),
    })
}

/// one json string value, by key. the bodies here are small and flat, and a
/// parser is a dependency this binary would otherwise not have.
fn field(body: &str, key: &str) -> String {
    let at = body
        .find(&format!("\"{key}\":\""))
        .unwrap_or_else(|| panic!("no {key} in {body}"));
    let rest = &body[at + key.len() + 4..];
    rest[..rest.find('"').unwrap()].to_string()
}

fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(got) = f() {
            return got;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// kill the deployment and read both its streams to the end.
fn stop(mut child: Child) -> (String, String) {
    let _ = child.kill();
    let mut out = String::new();
    let mut err = String::new();
    if let Some(stream) = child.stdout.take() {
        let _ = BufReader::new(stream).read_to_string(&mut out);
    }
    if let Some(stream) = child.stderr.take() {
        let _ = BufReader::new(stream).read_to_string(&mut err);
    }
    let _ = child.wait();
    (out, err)
}

/// the lines around a match, so a failure says where the leak is rather than
/// printing a megabyte of log.
fn context(text: &str, secret: &str) -> String {
    text.lines()
        .filter(|line| line.contains(secret))
        .take(5)
        .collect::<Vec<_>>()
        .join("\n")
}

fn find(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn exe() -> std::path::PathBuf {
    std::env::current_exe().expect("the test binary knows where it is")
}

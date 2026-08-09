use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};

/// what an [`IoManager`] returns from `put` and takes back in `get`.
pub type IoResult = Result<Value, Box<dyn std::error::Error + Send + Sync>>;

/// which op's output is being persisted or read back.
#[derive(Debug, Clone)]
pub struct IoKey {
    pub run_id: String,
    pub job: String,
    /// the op's name — `{op}[{i}]` for one fan-out instance, since each
    /// instance persists its own output.
    pub op: String,
}

/// where op outputs live. the default keeps them in the run log as json,
/// which is wrong for anything bulky; a manager is how you move them
/// somewhere that isn't sqlite while `op_runs.output` keeps a handle.
///
/// ## The contract
///
/// `put` persists a value and returns the **handle** recorded in
/// `op_runs.output`; `get` turns a handle back into the value. between them
/// they must round-trip: `get(key, put(key, v)) == v`.
///
/// `get` must also be **total**. it is called on every value a run hands an
/// op, and not all of them came from this manager's `put`: a source asset is
/// seeded `null`, a fan-out's collected array is assembled from its instances,
/// and a job can mix managers op by op. anything it did not produce, it must
/// return unchanged — which is exactly what [`Inline`] does with everything.
///
/// both are synchronous and run on the run's own task, so a manager that
/// talks to something slow should say so in its docs.
pub trait IoManager: Send + Sync + 'static {
    /// persist a value, returning the handle stored in `op_runs.output`.
    fn put(&self, key: &IoKey, value: Value) -> IoResult;
    /// resolve a handle back to the value.
    fn get(&self, key: &IoKey, handle: &Value) -> IoResult;
}

/// the default: outputs are their own handles, so they land in the run log
/// as json exactly as they always have.
pub struct Inline;

impl IoManager for Inline {
    fn put(&self, _key: &IoKey, value: Value) -> IoResult {
        Ok(value)
    }

    fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
        Ok(handle.clone())
    }
}

/// the tag on a [`FileIo`] handle. a handle is an object rather than a bare
/// path so anything reading `op_runs.output` can tell a reference from a
/// value at a glance — including the ui.
const FILE_TAG: &str = "file";

/// outputs written to one json file per op under `dir`, as
/// `{dir}/{run_id}/{op}.json`, with `{"$io": "file", "path": ".."}` recorded
/// in the run log.
///
/// nothing is ever cleaned up: [retention](crate::Store) prunes run rows, not
/// files. point it at a directory you are willing to sweep.
pub struct FileIo {
    dir: PathBuf,
}

impl FileIo {
    pub fn new(dir: impl Into<PathBuf>) -> FileIo {
        FileIo { dir: dir.into() }
    }

    fn path(&self, key: &IoKey) -> PathBuf {
        // an instance's `[` and `]` are fine on every filesystem hestan runs
        // on, and keeping the op's own name is worth more than sanitizing
        self.dir.join(&key.run_id).join(format!("{}.json", key.op))
    }
}

impl IoManager for FileIo {
    fn put(&self, key: &IoKey, value: Value) -> IoResult {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, serde_json::to_vec(&value)?)?;
        Ok(json!({ "$io": FILE_TAG, "path": path.to_string_lossy() }))
    }

    fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
        // anything that is not one of this manager's handles came from
        // somewhere else and is already the value
        let Some(path) = file_handle(handle) else {
            return Ok(handle.clone());
        };
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }
}

// the path inside a FileIo handle, if that is what this is
fn file_handle(handle: &Value) -> Option<&str> {
    let obj = handle.as_object()?;
    (obj.get("$io")?.as_str()? == FILE_TAG).then(|| obj.get("path")?.as_str())?
}

/// the managers a runner has: the default every op uses, plus the ones an op
/// can select by name with [`Op::io`](crate::Op::io).
#[derive(Clone)]
pub(crate) struct Io {
    default: Arc<dyn IoManager>,
    named: HashMap<String, Arc<dyn IoManager>>,
}

impl Default for Io {
    fn default() -> Io {
        Io {
            default: Arc::new(Inline),
            named: HashMap::new(),
        }
    }
}

impl Io {
    pub(crate) fn new(
        default: Option<Arc<dyn IoManager>>,
        named: HashMap<String, Arc<dyn IoManager>>,
    ) -> Io {
        Io {
            default: default.unwrap_or_else(|| Arc::new(Inline)),
            named,
        }
    }

    pub(crate) fn knows(&self, name: &str) -> bool {
        self.named.contains_key(name)
    }

    /// the manager for an op that selected `name`, or the default. an
    /// unknown name cannot get here — the build refuses it — but falling back
    /// beats panicking in a run loop.
    pub(crate) fn manager(&self, name: Option<&str>) -> &dyn IoManager {
        name.and_then(|n| self.named.get(n))
            .unwrap_or(&self.default)
            .as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(run: &str, op: &str) -> IoKey {
        IoKey {
            run_id: run.into(),
            job: "j".into(),
            op: op.into(),
        }
    }

    #[test]
    fn inline_is_the_identity_both_ways() {
        let io = Inline;
        let k = key("r1", "a");
        let v = json!({"rows": [1, 2, 3]});
        let handle = io.put(&k, v.clone()).unwrap();
        assert_eq!(handle, v);
        assert_eq!(io.get(&k, &handle).unwrap(), v);
    }

    #[test]
    fn file_io_round_trips_through_a_file_at_the_expected_path() {
        let dir = tempfile::tempdir().unwrap();
        let io = FileIo::new(dir.path());
        let k = key("r1", "extract");
        let v = json!({"rows": [1, 2, 3]});

        let handle = io.put(&k, v.clone()).unwrap();
        let path = dir.path().join("r1").join("extract.json");
        assert_eq!(handle["$io"], "file");
        assert_eq!(handle["path"], path.to_string_lossy().as_ref());
        assert!(path.exists(), "no file at {path:?}");
        assert_eq!(io.get(&k, &handle).unwrap(), v);
    }

    // a run mixes managers and seeds values that never went through one
    #[test]
    fn file_io_passes_through_what_it_did_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let io = FileIo::new(dir.path());
        let k = key("r1", "a");
        for v in [json!(null), json!([1, 2]), json!({"$io": "s3"}), json!("x")] {
            assert_eq!(io.get(&k, &v).unwrap(), v);
        }
    }

    #[test]
    fn a_put_that_cannot_write_is_an_error_rather_than_a_lost_output() {
        let dir = tempfile::tempdir().unwrap();
        // a file where the run's directory needs to be
        let blocked = dir.path().join("blocked");
        fs::write(&blocked, b"not a directory").unwrap();
        let io = FileIo::new(dir.path());
        assert!(io.put(&key("blocked", "a"), json!(1)).is_err());
    }
}

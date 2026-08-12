use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};

use crate::op::Meta;

/// what an [`IoManager`] returns from `put` and takes back in `get`.
pub type IoResult = Result<Value, Box<dyn std::error::Error + Send + Sync>>;

/// which op's output is being persisted or read back.
#[derive(Debug, Clone)]
pub struct IoKey {
    /// the run whose output this is. a manager that lays out storage by run
    /// can delete a run's outputs with one prefix.
    pub run_id: String,
    /// the job that run was of.
    pub job: String,
    /// the op's name — `{op}[{i}]` for one fan-out instance, since each
    /// instance persists its own output.
    pub op: String,
}

/// where op outputs live. the default keeps them in the run log as json,
/// which is wrong for anything bulky; a manager is how you move them out of
/// the run log altogether while `op_runs.output` keeps a handle.
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

/// where a manager that writes one file per op puts it —
/// `{dir}/{run_id}/{op}.{ext}` — or why that key does not name a file under
/// `dir` at all.
///
/// an asset's name is already a path: `sales/orders` is a directory and a
/// file here, and the catalog groups on the same prefix. so what is refused is
/// a name that leaves `dir`, not a name with a separator in it — every part of
/// both halves of the key has to be an ordinary path component.
///
/// both managers call this rather than each carrying a copy, because two
/// answers to one question is how the two of them drift.
fn contained(dir: &Path, key: &IoKey, ext: &str) -> Result<PathBuf, String> {
    let mut path = dir.join(relative("run id", &key.run_id)?);
    let mut file = relative("op", &key.op)?.into_os_string();
    // pushed onto the name rather than set as an extension, so an op called
    // `orders.2024` keeps what is after its dot
    file.push(".");
    file.push(ext);
    path.push(file);
    Ok(path)
}

/// `name` as a relative path of ordinary components, or why it is not one.
fn relative(what: &str, name: &str) -> Result<PathBuf, String> {
    let refused = || format!("{what} {name:?} does not name a file under the io directory");
    let mut out = PathBuf::new();
    for part in Path::new(name).components() {
        match part {
            // an instance's `[` and `]` are fine on every filesystem hestan
            // runs on, and so is the `/` an asset name carries
            Component::Normal(part) => out.push(part),
            // `./x` is `x`; every other kind either climbs out of the
            // directory or starts somewhere else entirely
            Component::CurDir => {}
            _ => return Err(refused()),
        }
    }
    match out.as_os_str().is_empty() {
        true => Err(refused()),
        false => Ok(out),
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
/// an op named for an asset takes the asset's name with it, so `sales/orders`
/// is a directory and a file under the run's. a name that would land outside
/// `dir` fails the op instead, which is the only thing refused here.
///
/// nothing is ever cleaned up: [retention](crate::Retention) prunes run rows,
/// not files. point it at a directory you are willing to sweep.
pub struct FileIo {
    dir: PathBuf,
}

impl FileIo {
    /// outputs under `dir`. the directory is created as runs need it, and
    /// nothing here ever removes one.
    pub fn new(dir: impl Into<PathBuf>) -> FileIo {
        FileIo { dir: dir.into() }
    }

    fn path(&self, key: &IoKey) -> Result<PathBuf, String> {
        contained(&self.dir, key, "json")
    }
}

impl IoManager for FileIo {
    fn put(&self, key: &IoKey, value: Value) -> IoResult {
        let path = self.path(key)?;
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

/// the path inside a handle tagged `tag`, if that is what this is.
#[cfg(feature = "parquet")]
fn tagged_handle<'a>(handle: &'a Value, tag: &str) -> Option<&'a str> {
    let obj = handle.as_object()?;
    (obj.get("$io")?.as_str()? == tag).then(|| obj.get("path")?.as_str())?
}

/// the two numbers a handle may carry: how many rows were stored and how big
/// the thing that holds them is.
///
/// a manager knows both and the op does not — it returned a value, not a file
/// — so this is where they come from. anything the op staged under the same
/// name wins, since an op saying `rows` means its own rows.
pub(crate) fn handle_meta(handle: &Value, staged: Option<Value>) -> Option<Value> {
    let Some(obj) = handle.as_object().filter(|o| o.contains_key("$io")) else {
        return staged;
    };
    let mut out = match staged {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    for (key, meta) in [
        ("rows", Meta::count as fn(u64) -> Meta),
        ("bytes", Meta::bytes),
    ] {
        if let Some(n) = obj.get(key).and_then(Value::as_u64)
            && !out.contains_key(key)
        {
            out.insert(key.to_string(), meta(n).tagged());
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

/// the tag on a [`ParquetIo`] handle, beside the row and byte counts that
/// become the op's [metadata](handle_meta).
#[cfg(feature = "parquet")]
const PARQUET_TAG: &str = "parquet";

/// outputs written as one parquet file per op under `dir`, as
/// `{dir}/{run_id}/{op}.parquet`, with
/// `{"$io": "parquet", "path": .., "rows": .., "bytes": ..}` recorded in the
/// run log. the row and byte counts land on the op run as
/// [`Meta::Count`](crate::Meta::Count) and [`Meta::Bytes`](crate::Meta::Bytes)
/// without the op asking.
///
/// ```no_run
/// # use hestan::{Hestan, Op, ParquetIo};
/// Hestan::new().io_named("parquet", ParquetIo::new("/var/lib/hestan/parquet"));
/// ```
///
/// ## What it stores
///
/// **a table**: a json array whose elements are objects, one per row. that is
/// what an op returns when it returns rows — including a
/// [`typed`](crate::Op::typed) op returning a `Vec<T>`. the column types are
/// inferred from the values: whole numbers as `int64`, fractions as `float64`,
/// then `utf8`, `bool`, lists and structs, and a column that is null the whole
/// way down as parquet's null type.
///
/// `null` is passed through untouched — an op that produced nothing has no
/// table to write, and null is already its own handle. anything else is an
/// error rather than a silent fallback to json: an op stored somewhere it did
/// not ask for is a value nobody finds again.
///
/// names land where [`FileIo`]'s do, and are refused on the same terms: an
/// asset called `sales/orders` is a directory and a file under the run's, and
/// a name that would leave `dir` fails the op.
///
/// two things do not survive the round trip and neither can:
///
/// - a column mixing whole numbers and fractions is one `float64` column, so
///   `1` reads back as `1.0`. a parquet column has one type; json does not.
/// - a key missing from one row reads back as an explicit `null`, because a
///   table has the same columns in every row.
///
/// ## What it is not
///
/// a directory of files, exactly as [`FileIo`] is one. no partitioned
/// datasets, no compaction, no manifest, no object store — an op writes one
/// file and the op downstream reads that file. anything more is a table
/// format, which is a different thing to be.
///
/// and **nothing is ever cleaned up**: [retention](crate::Retention) prunes
/// run rows, not files, so a pruned run leaves its parquet behind exactly as
/// `FileIo` leaves its json. point it at a directory you are willing to sweep.
///
/// reading and writing happen on the run's own task, since that is what the
/// [`IoManager`] contract is. a file worth minutes of io is worth doing in the
/// op instead.
#[cfg(feature = "parquet")]
#[cfg_attr(docsrs, doc(cfg(feature = "parquet")))]
pub struct ParquetIo {
    dir: PathBuf,
}

#[cfg(feature = "parquet")]
impl ParquetIo {
    /// outputs under `dir`. the directory is created as runs need it, and
    /// nothing here ever removes one.
    pub fn new(dir: impl Into<PathBuf>) -> ParquetIo {
        ParquetIo { dir: dir.into() }
    }

    fn path(&self, key: &IoKey) -> Result<PathBuf, String> {
        contained(&self.dir, key, "parquet")
    }
}

#[cfg(feature = "parquet")]
impl IoManager for ParquetIo {
    fn put(&self, key: &IoKey, value: Value) -> IoResult {
        if value.is_null() {
            return Ok(value);
        }
        let path = self.path(key)?;
        let rows = parquet_impl::rows(&value)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        parquet_impl::write(&path, rows)?;
        Ok(json!({
            "$io": PARQUET_TAG,
            "path": path.to_string_lossy(),
            "rows": rows.len(),
            "bytes": fs::metadata(&path)?.len(),
        }))
    }

    fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
        // anything that is not one of this manager's handles came from
        // somewhere else and is already the value
        let Some(path) = tagged_handle(handle, PARQUET_TAG) else {
            return Ok(handle.clone());
        };
        Ok(parquet_impl::read(path.as_ref())?)
    }
}

/// json rows through arrow and back. kept apart from the manager so the
/// arrow-shaped half of this is one screen you can read on its own.
#[cfg(feature = "parquet")]
mod parquet_impl {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow::error::ArrowError;
    use arrow::json::ReaderBuilder;
    use arrow::json::reader::infer_json_schema_from_iterator;
    use arrow::json::writer::{JsonArray, WriterBuilder};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use serde_json::Value;

    /// the rows in an output, or why it is not a table.
    pub(super) fn rows(value: &Value) -> Result<&Vec<Value>, String> {
        let Some(rows) = value.as_array() else {
            return Err(format!(
                "parquet stores a table: an array of row objects, not {}",
                kind(value)
            ));
        };
        match rows.iter().position(|row| !row.is_object()) {
            Some(i) => Err(format!(
                "parquet stores a table: row {i} is {}, not an object",
                kind(&rows[i])
            )),
            None => Ok(rows),
        }
    }

    /// what a value is, for a message somebody has to act on.
    fn kind(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "a boolean",
            Value::Number(_) => "a number",
            Value::String(_) => "a string",
            Value::Array(_) => "an array",
            Value::Object(_) => "an object",
        }
    }

    /// write the rows as one parquet file.
    ///
    /// the schema is inferred from the rows themselves — every one of them,
    /// not the first: a column that is null until row four is still that
    /// column's type, and inferring from a sample is how a load fails at 3am
    /// on the one row that was different.
    pub(super) fn write(path: &Path, rows: &[Value]) -> Result<(), ArrowError> {
        let schema = Arc::new(infer_json_schema_from_iterator(rows.iter().map(Ok))?);
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
        decoder.serialize(rows)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        while let Some(batch) = decoder.flush()? {
            writer
                .write(&batch)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        }
        // the footer holds the schema and the row group index, so a file that
        // was never closed is a file no reader can open
        writer
            .close()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        Ok(())
    }

    /// read one back as the rows it was written from.
    ///
    /// nulls are written out explicitly rather than left off the object: an
    /// absent key and a null one are different values to whoever reads the
    /// output next, and only one of them is what was stored.
    pub(super) fn read(path: &Path) -> Result<Value, ArrowError> {
        let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
            .build()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        let mut out: Vec<u8> = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(&mut out);
        for batch in batches {
            writer.write(&batch.map_err(|e| ArrowError::ExternalError(Box::new(e)))?)?;
        }
        writer.finish()?;
        serde_json::from_slice(&out).map_err(|e| ArrowError::JsonError(e.to_string()))
    }
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

    /// every name the managers have to agree about, and whether a file may be
    /// written for it. the property is that the file lands under the directory
    /// the manager was given — not that the name is a boring one, since an
    /// asset's name is a path and the catalog reads it as one.
    fn names() -> [(&'static str, bool); 10] {
        [
            ("extract", true),
            ("sales/orders", true),
            ("fetch[0]", true),
            ("./deep/nested", true),
            ("orders.2024", true),
            ("../escape", false),
            ("sales/../../escape", false),
            ("/etc/hestan", false),
            ("..", false),
            ("", false),
        ]
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

    #[test]
    fn file_io_writes_under_its_own_directory_whatever_the_key_says() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("io");
        let io = FileIo::new(&root);
        for (name, allowed) in names() {
            // both halves of the key, because a run id is a path component too
            for k in [key(name, "extract"), key("r1", name)] {
                let v = json!({ "name": name });
                if !allowed {
                    let err = io.put(&k, v).err().unwrap().to_string();
                    assert!(err.contains("does not name a file"), "{k:?}: {err}");
                    continue;
                }
                let handle = io
                    .put(&k, v.clone())
                    .unwrap_or_else(|e| panic!("{k:?}: {e}"));
                let path = PathBuf::from(handle["path"].as_str().unwrap());
                assert!(path.starts_with(&root), "{k:?} wrote {path:?}");
                assert_eq!(io.get(&k, &handle).unwrap(), v);
            }
        }
        // and the directory it was given is the only thing that grew
        let beside: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(beside, ["io"]);
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

    #[test]
    fn a_handle_saying_how_much_it_stored_becomes_the_ops_metadata() {
        let handle = json!({"$io": "parquet", "path": "/x.parquet", "rows": 3, "bytes": 900});
        let meta = handle_meta(&handle, None).unwrap();
        assert_eq!(meta["rows"], json!({"count": 3}));
        assert_eq!(meta["bytes"], json!({"bytes": 900}));

        // what the op said about its own rows is what it meant by rows
        let staged = json!({"rows": {"count": 7}});
        let meta = handle_meta(&handle, Some(staged)).unwrap();
        assert_eq!(meta["rows"], json!({"count": 7}));
        assert_eq!(meta["bytes"], json!({"bytes": 900}));

        // an inline output is a value and not a handle, whatever keys it has
        let output = json!({"rows": 3, "bytes": 900});
        assert_eq!(handle_meta(&output, None), None);
        assert_eq!(handle_meta(&json!(null), None), None);
        // and a manager that counts nothing changes nothing
        assert_eq!(
            handle_meta(&json!({"$io": "file", "path": "/x"}), None),
            None
        );
    }

    #[cfg(feature = "parquet")]
    mod parquet {
        use super::*;

        /// one row of every family json has, so that a column of each kind
        /// has to survive the trip.
        fn rows() -> Value {
            json!([
                {
                    "id": 1,
                    "name": "ana",
                    "ratio": 1.5,
                    "ok": true,
                    "note": null,
                    "tags": ["eu", "vip"],
                    "address": {"city": "porto", "zip": null},
                    "never_set": null,
                },
                {
                    "id": -2,
                    "name": null,
                    "ratio": -0.25,
                    "ok": false,
                    "note": "second",
                    "tags": [],
                    "address": {"city": "oslo", "zip": "0150"},
                    "never_set": null,
                },
            ])
        }

        #[test]
        fn parquet_round_trips_a_table_of_every_type_including_its_nulls() {
            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            let k = key("r1", "extract");
            let v = rows();

            let handle = io.put(&k, v.clone()).unwrap();
            let path = dir.path().join("r1").join("extract.parquet");
            assert_eq!(handle["$io"], "parquet");
            assert_eq!(handle["path"], path.to_string_lossy().as_ref());
            assert_eq!(handle["rows"], json!(2));
            assert_eq!(handle["bytes"], json!(fs::metadata(&path).unwrap().len()));
            // value for value, nulls included: an absent key and a null one
            // are different things to whatever reads this next
            assert_eq!(io.get(&k, &handle).unwrap(), v);
        }

        // the point of storing a column format is that it is one: a file only
        // hestan could read back would be json with extra steps
        #[test]
        fn the_file_is_parquet_with_a_column_of_the_type_each_value_was() {
            use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            io.put(&key("r1", "extract"), rows()).unwrap();
            let path = dir.path().join("r1").join("extract.parquet");

            let bytes = fs::read(&path).unwrap();
            assert_eq!(&bytes[..4], b"PAR1", "no parquet magic at the front");
            assert_eq!(&bytes[bytes.len() - 4..], b"PAR1", "no footer magic");

            let schema = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap())
                .unwrap()
                .schema()
                .clone();
            let types: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("{}: {}", f.name(), f.data_type()))
                .collect();
            assert_eq!(
                types,
                [
                    // inference sorts the columns; the order is arrow's, and
                    // an object compares by key either way
                    r#"address: Struct("city": Utf8, "zip": Utf8)"#,
                    "id: Int64",
                    "name: Utf8",
                    "never_set: Null",
                    "note: Utf8",
                    "ok: Boolean",
                    "ratio: Float64",
                    "tags: List(Utf8)",
                ]
            );
        }

        // a run mixes managers and seeds values that never went through one
        #[test]
        fn parquet_passes_through_what_it_did_not_write() {
            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            let k = key("r1", "a");
            for v in [
                json!(null),
                json!([{"a": 1}]),
                json!({"$io": "file", "path": "/tmp/x.json"}),
                json!("x"),
            ] {
                assert_eq!(io.get(&k, &v).unwrap(), v);
            }
        }

        #[test]
        fn an_output_that_is_not_a_table_is_refused_rather_than_stored_anyway() {
            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            let k = key("r1", "load");
            let err = |v: Value| io.put(&k, v).unwrap_err().to_string();
            assert_eq!(
                err(json!({"loaded": 4210})),
                "parquet stores a table: an array of row objects, not an object"
            );
            assert_eq!(
                err(json!([{"a": 1}, 7])),
                "parquet stores a table: row 1 is a number, not an object"
            );
            // and nothing was written for any of them
            assert!(!dir.path().join("r1").exists());
        }

        // the op that returns `Ok(json!(null))` is every op that loads
        // something somewhere else, and it has no table to write
        #[test]
        fn an_op_that_produced_nothing_is_its_own_handle() {
            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            let k = key("r1", "load");
            let handle = io.put(&k, json!(null)).unwrap();
            assert_eq!(handle, json!(null));
            assert_eq!(io.get(&k, &handle).unwrap(), json!(null));
            assert!(!dir.path().join("r1").exists());
        }

        // no rows is a result: a query that matched nothing is not a failure,
        // and the run after it should read an empty table rather than an error
        #[test]
        fn a_query_that_matched_nothing_stores_a_file_that_reads_back_empty() {
            let dir = tempfile::tempdir().unwrap();
            let io = ParquetIo::new(dir.path());
            let k = key("r1", "extract");
            let handle = io.put(&k, json!([])).unwrap();
            assert_eq!(handle["rows"], json!(0));
            assert!(dir.path().join("r1").join("extract.parquet").exists());
            assert_eq!(io.get(&k, &handle).unwrap(), json!([]));
        }

        #[test]
        fn a_put_that_cannot_write_is_an_error_rather_than_a_lost_output() {
            let dir = tempfile::tempdir().unwrap();
            let blocked = dir.path().join("blocked");
            fs::write(&blocked, b"not a directory").unwrap();
            let io = ParquetIo::new(dir.path());
            assert!(io.put(&key("blocked", "a"), json!([{"a": 1}])).is_err());
        }

        // the same table the json manager is held to, because a key that is a
        // file here and an error there is two answers to one question
        #[test]
        fn parquet_writes_under_its_own_directory_whatever_the_key_says() {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("io");
            let io = ParquetIo::new(&root);
            for (name, allowed) in names() {
                for k in [key(name, "extract"), key("r1", name)] {
                    let v = json!([{ "name": name }]);
                    if !allowed {
                        let err = io.put(&k, v).err().unwrap().to_string();
                        assert!(err.contains("does not name a file"), "{k:?}: {err}");
                        continue;
                    }
                    let handle = io
                        .put(&k, v.clone())
                        .unwrap_or_else(|e| panic!("{k:?}: {e}"));
                    let path = PathBuf::from(handle["path"].as_str().unwrap());
                    assert!(path.starts_with(&root), "{k:?} wrote {path:?}");
                    assert_eq!(io.get(&k, &handle).unwrap(), v);
                }
            }
            let beside: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(beside, ["io"]);
        }
    }
}

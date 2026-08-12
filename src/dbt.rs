//! a dbt project's models as [assets](crate::Asset), with dbt's own lineage.
//!
//! dbt has already worked out what depends on what and written it down in
//! `target/manifest.json`. this reads that file and produces one asset per
//! model, wired from the manifest's own `depends_on` — so the graph on
//! hestan's asset page is the graph dbt compiled, rather than one somebody
//! retyped and will forget to update.
//!
//! ```no_run
//! # use hestan::{Hestan, dbt::Dbt};
//! # async fn f() -> Result<(), hestan::Error> {
//! let dbt = Dbt::from_manifest("analytics/target/manifest.json")?;
//! Hestan::new()
//!     .assets(dbt.assets())
//!     .serve(([127, 0, 0, 1], 4000))
//!     .await
//! # }
//! ```
//!
//! building one of those assets runs `dbt run --select <model>` in the project
//! directory and stores what it printed as the op's
//! [output](crate::Store::op_logs). **your dbt, your profile, invoked** —
//! nothing here reimplements a jinja renderer, an adapter or a profile
//! lookup, and nothing here parses dbt's sql. hestan is the thing that decides
//! when a model is built and records what happened when it was.
//!
//! what that buys over `dbt run` on a cron: a model is a node in the same
//! graph as everything else you orchestrate, so an asset of your own can
//! depend on `orders_daily` and be built when it is, and the run page shows
//! which model failed rather than one exit code for the lot.
//!
//! # Manifest versions
//!
//! this reads **manifest schema v9 through v12**, which is dbt 1.5 through
//! 1.10. the four fields it takes out of a manifest — a node's `name`,
//! `resource_type` and `depends_on.nodes`, and a source's `source_name` —
//! have meant the same thing across all of them, and everything else in the
//! file is ignored, so a version that only adds fields keeps working.
//!
//! anything outside that range is [`crate::Error::Dbt`] naming
//! the version it found. the alternative — parsing hopefully — produces an
//! empty asset graph, which looks exactly like a project nobody has compiled
//! yet, and that is a failure somebody debugs for an afternoon.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;
use tokio::process::Command;

use crate::asset::Asset;
use crate::error::Error;
use crate::logs::{Attempt, capture_child};
use crate::op::{OpCtx, OpResult};

/// the `manifest.json` schema versions this reads — see the [module
/// docs](self#manifest-versions), which is where a reader looks for them.
const SCHEMA_VERSIONS: std::ops::RangeInclusive<u32> = 9..=12;

/// what a model's asset returns, and what `dbt` is called by default.
const PROGRAM: &str = "dbt";

/// how long a finished `dbt` process's pipes are read for after it exits.
/// only a grandchild it left behind holding one open can reach this, and a
/// lost tail of output beats an op that never ends.
const CAPTURE_GRACE: Duration = Duration::from_secs(3);

/// a parsed dbt manifest: the models it defines, the sources they read, and
/// how to invoke dbt for them.
///
/// build it with [`from_manifest`](Dbt::from_manifest) and hand
/// [`assets`](Dbt::assets) to [`Hestan::assets`](crate::Hestan::assets).
///
/// the `Debug` is the parsed graph, which is what you want when the assets
/// are not the ones you expected.
#[derive(Debug)]
pub struct Dbt {
    project_dir: PathBuf,
    program: String,
    /// in manifest order, which is by unique id: the same graph every time
    /// this is read, so the catalog does not reshuffle between boots.
    models: Vec<Model>,
    /// the sources at least one model reads, as asset names.
    sources: Vec<String>,
}

/// one dbt model: what hestan calls it, and what it depends on.
#[derive(Debug)]
struct Model {
    /// the model's name, which is both the asset's name and the selector
    /// `dbt run --select` is given.
    name: String,
    deps: Vec<String>,
}

impl Dbt {
    /// read `target/manifest.json` — the file `dbt compile`, `dbt run` and
    /// `dbt parse` all write.
    ///
    /// the project directory defaults to two levels up from the manifest,
    /// since dbt writes `<project>/target/manifest.json`;
    /// [`project_dir`](Dbt::project_dir) says otherwise.
    ///
    /// the errors are all one variant, [`Error::Dbt`], and each names the file:
    /// it cannot be read, it is not json, its schema version is not one of
    /// [the versions this reads](self#manifest-versions), or two of its nodes
    /// would become one asset.
    pub fn from_manifest(path: impl AsRef<Path>) -> Result<Dbt, Error> {
        let path = path.as_ref();
        let refused = |reason: String| Error::Dbt {
            path: path.display().to_string(),
            reason,
        };
        let bytes = std::fs::read(path).map_err(|e| refused(format!("could not read it: {e}")))?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|e| refused(format!("could not parse it as a dbt manifest: {e}")))?;
        let declared = &manifest.metadata.dbt_schema_version;
        let version = schema_version(declared).ok_or_else(|| {
            refused(format!(
                "its schema version is {declared:?}, which does not name a version at all"
            ))
        })?;
        if !SCHEMA_VERSIONS.contains(&version) {
            return Err(refused(format!(
                "it is manifest schema v{version}, and this build of hestan reads v{} to v{} \
                 (dbt 1.5 to 1.10)",
                SCHEMA_VERSIONS.start(),
                SCHEMA_VERSIONS.end()
            )));
        }

        // every node that becomes an asset, by the unique id `depends_on`
        // names it with. sources first: a model and a source cannot collide,
        // because a source's asset name carries its source
        let mut named: HashMap<&str, String> = HashMap::new();
        let mut taken: HashMap<String, &str> = HashMap::new();
        for (id, source) in &manifest.sources {
            named.insert(id, format!("{}.{}", source.source_name, source.name));
        }
        for (id, node) in &manifest.nodes {
            if node.resource_type != "model" {
                continue;
            }
            // two models of the same name in two packages would be one asset,
            // and quietly keeping the second would drop the first's lineage
            if let Some(first) = taken.insert(node.name.clone(), id) {
                return Err(refused(format!(
                    "{first} and {id} would both be the asset {:?}",
                    node.name
                )));
            }
            named.insert(id, node.name.clone());
        }

        let mut models = Vec::new();
        let mut sources: Vec<String> = Vec::new();
        for (id, node) in &manifest.nodes {
            if node.resource_type != "model" {
                continue;
            }
            let mut deps: Vec<String> = Vec::new();
            for dep in &node.depends_on.nodes {
                // a dep on a seed, a snapshot or anything else that is not a
                // model or a source is not lineage hestan has a node for
                let Some(name) = named.get(dep.as_str()) else {
                    continue;
                };
                if !deps.contains(name) {
                    deps.push(name.clone());
                }
                if dep.starts_with("source.") && !sources.contains(name) {
                    sources.push(name.clone());
                }
            }
            models.push(Model {
                name: named[id.as_str()].clone(),
                deps,
            });
        }
        sources.sort();

        Ok(Dbt {
            project_dir: path
                .parent()
                .and_then(Path::parent)
                .unwrap_or(Path::new("."))
                .to_path_buf(),
            program: PROGRAM.to_string(),
            models,
            sources,
        })
    }

    /// where dbt is run: the directory holding `dbt_project.yml`.
    pub fn project_dir(mut self, dir: impl Into<PathBuf>) -> Dbt {
        self.project_dir = dir.into();
        self
    }

    /// which executable is dbt, for a dbt that is not on `PATH` as `dbt` — a
    /// virtualenv's, or a wrapper of your own. one program and no arguments:
    /// the arguments are hestan's, and they are `run --select <model>`.
    pub fn command(mut self, program: impl Into<String>) -> Dbt {
        self.program = program.into();
        self
    }

    /// one asset per model, plus a [source](crate::Asset::source) asset for
    /// each source a model reads.
    ///
    /// a source arrives with no [probe](crate::Asset::probe), so nothing
    /// upstream ever marks a model stale on its own: hestan does not query
    /// your warehouse and will not pretend to know when a table last changed.
    /// give one a probe of your own — [`Asset::name`](crate::Asset::name)
    /// finds it in this vec — if you want the freshness of a source to drive
    /// rebuilds.
    ///
    /// a source nothing reads is not in here. it is a line in a yml file, not
    /// a thing anything is made of.
    pub fn assets(&self) -> Vec<Asset> {
        let sources = self.sources.iter().map(Asset::source);
        sources
            .chain(self.models.iter().map(|m| self.model(m)))
            .collect()
    }

    /// the asset one model becomes: its dbt deps, and a body that invokes dbt
    /// for exactly this model.
    fn model(&self, model: &Model) -> Asset {
        let (program, dir, select) = (
            self.program.clone(),
            self.project_dir.clone(),
            model.name.clone(),
        );
        let asset = Asset::new(model.name.clone(), move |ctx: OpCtx| {
            let (program, dir, select) = (program.clone(), dir.clone(), select.clone());
            async move { build(ctx, program, dir, select).await }
        });
        model
            .deps
            .iter()
            .fold(asset, |asset, dep| asset.from_named(dep))
    }
}

/// build one model: invoke dbt, keep what it printed, fail on what it exited
/// with.
async fn build(ctx: OpCtx, program: String, dir: PathBuf, select: String) -> OpResult {
    ctx.info(format!("{program} run --select {select}"));
    let status = run(&ctx, &program, &dir, &select)
        .await
        .map_err(|e| format!("could not start {program} in {}: {e}", dir.display()))?;
    if !status.success() {
        return Err(match status.code() {
            Some(code) => format!("{program} run --select {select} exited {code}"),
            None => format!("{program} run --select {select} was killed"),
        }
        .into());
    }
    // dbt rebuilt the table and hestan cannot see inside it, so the
    // fingerprint says which build this was rather than what is in it. that
    // is what makes everything downstream of a rebuilt model stale, and a
    // model nobody rebuilt fresh
    ctx.set_fingerprint(format!("{}:{}", ctx.run_id(), select));
    Ok(json!({ "model": select }))
}

/// spawn dbt with both pipes read into this attempt's captured output.
///
/// the same capture an [isolated op](crate::Op::isolated)'s subprocess gets,
/// for the same reason: what a child process prints is the op's output and
/// belongs on the run page rather than on whatever terminal the orchestrator
/// happens to have. dbt is chatty and the interesting line is always in the
/// middle of it.
async fn run(
    ctx: &OpCtx,
    program: &str,
    dir: &Path,
    select: &str,
) -> std::io::Result<std::process::ExitStatus> {
    let mut child = Command::new(program)
        .args(["run", "--select", select])
        .current_dir(dir)
        // dbt asks nothing interactively and a child inheriting this
        // process's stdin can block a run on a terminal nobody is at
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // a canceled run drops this future, and a dropped child left running
        // would keep writing to a warehouse for a run that is over
        .kill_on_drop(true)
        .spawn()?;
    // which attempt this is: the row carries it, and a retry's output belongs
    // under the retry rather than beside the attempt that failed
    let attempt = ctx
        .store
        .op_run(ctx.run_id(), &ctx.op)
        .ok()
        .flatten()
        .map_or(1, |row| row.attempts);
    let capture = capture_child(
        &mut child,
        &ctx.store,
        &Attempt::new(ctx.run_id(), &ctx.op, attempt),
    );
    let status = child.wait().await;
    // after the exit, because a pipe reaches its end when the process holding
    // the far side of it is gone
    capture.finish(CAPTURE_GRACE).await;
    status
}

/// the version in `https://schemas.getdbt.com/dbt/manifest/v12.json`.
fn schema_version(declared: &str) -> Option<u32> {
    declared
        .rsplit('/')
        .next()?
        .strip_prefix('v')?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

/// the whole of what hestan reads out of a manifest. everything else in the
/// file — and there is a great deal of it — is somebody else's business, and
/// serde ignores it, so a version that adds a field is not a version this
/// stops reading.
#[derive(Deserialize)]
struct Manifest {
    metadata: Metadata,
    #[serde(default)]
    nodes: BTreeMap<String, Node>,
    #[serde(default)]
    sources: BTreeMap<String, Source>,
}

#[derive(Deserialize)]
struct Metadata {
    dbt_schema_version: String,
}

#[derive(Deserialize)]
struct Node {
    name: String,
    resource_type: String,
    #[serde(default)]
    depends_on: DependsOn,
}

#[derive(Deserialize, Default)]
struct DependsOn {
    #[serde(default)]
    nodes: Vec<String>,
}

#[derive(Deserialize)]
struct Source {
    name: String,
    source_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt/target/manifest.json")
    }

    fn project() -> Dbt {
        Dbt::from_manifest(fixture()).unwrap()
    }

    /// `(name, deps)` for every asset, in the order they are registered.
    fn graph(dbt: &Dbt) -> Vec<(String, Vec<String>)> {
        dbt.assets()
            .iter()
            .map(|a| (a.name().to_string(), a.deps().to_vec()))
            .collect()
    }

    #[test]
    fn every_model_is_an_asset_wired_the_way_the_manifest_wires_it() {
        assert_eq!(
            graph(&project()),
            [
                ("raw.orders".to_string(), vec![]),
                (
                    "orders_by_region".to_string(),
                    vec!["stg_orders".to_string()]
                ),
                ("orders_daily".to_string(), vec!["stg_orders".to_string()]),
                (
                    "orders_summary".to_string(),
                    vec!["orders_daily".to_string(), "orders_by_region".to_string()]
                ),
                ("stg_orders".to_string(), vec!["raw.orders".to_string()]),
            ]
        );
    }

    // the manifest holds seeds, tests, hooks and whatever the next version of
    // dbt adds beside them. a test node that became an asset would put a data
    // test in the catalog as something you can build
    #[test]
    fn a_node_that_is_not_a_model_is_not_an_asset() {
        let names: Vec<String> = graph(&project()).into_iter().map(|(n, _)| n).collect();
        for absent in [
            "region_codes",           // a seed
            "not_null_stg_orders_id", // a data test
            "shop-on-run-start-0",    // a hook
            "orders_legacy",          // disabled, so not in `nodes` at all
            "raw.shipments",          // a source nothing reads
        ] {
            assert!(!names.contains(&absent.to_string()), "{absent} is an asset");
        }
        // and the dep on the seed is dropped rather than dangling
        assert_eq!(
            graph(&project())
                .into_iter()
                .find(|(n, _)| n == "orders_by_region")
                .unwrap()
                .1,
            ["stg_orders"]
        );
    }

    // the assets have to be a graph hestan will actually register: a dep on a
    // name nothing produces is a startup error, and that is the whole claim
    // this part makes — dbt's lineage, inside hestan's
    #[test]
    fn the_assets_register_as_one_graph() {
        crate::asset::AssetRegistry::new(project().assets(), Vec::new(), Vec::new()).unwrap();
    }

    #[test]
    fn the_project_is_where_dbt_wrote_the_manifest_from() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt");
        assert_eq!(project().project_dir, root);
        assert_eq!(project().program, "dbt");
        let moved = project().project_dir("/srv/analytics").command("uv-dbt");
        assert_eq!(moved.project_dir, PathBuf::from("/srv/analytics"));
        assert_eq!(moved.program, "uv-dbt");
    }

    /// a manifest of this shape, written where a test can read it back.
    fn written(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("manifest.json");
        std::fs::write(&path, body).unwrap();
        path
    }

    // the failure this exists to prevent: a manifest hestan does not
    // understand parsing to no models at all, which looks exactly like a
    // project somebody has not compiled yet
    #[test]
    fn a_schema_version_this_build_does_not_read_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = |version: &str| {
            format!(
                r#"{{"metadata": {{"dbt_schema_version": "{version}"}},
                    "nodes": {{}}, "sources": {{}}}}"#
            )
        };
        let of = |version: &str| {
            let path = written(dir.path(), &manifest(version));
            Dbt::from_manifest(path).unwrap_err().to_string()
        };

        let older = of("https://schemas.getdbt.com/dbt/manifest/v4.json");
        assert!(
            older.contains("manifest schema v4") && older.contains("reads v9 to v12"),
            "{older}"
        );
        let newer = of("https://schemas.getdbt.com/dbt/manifest/v14.json");
        assert!(newer.contains("manifest schema v14"), "{newer}");
        let nonsense = of("manifest.json");
        assert!(
            nonsense.contains("does not name a version at all"),
            "{nonsense}"
        );

        // and the versions in the range are read, empty project and all
        for known in ["v9", "v10", "v11", "v12"] {
            let path = written(
                dir.path(),
                &manifest(&format!(
                    "https://schemas.getdbt.com/dbt/manifest/{known}.json"
                )),
            );
            assert!(Dbt::from_manifest(path).unwrap().assets().is_empty());
        }
    }

    #[test]
    fn a_manifest_that_is_not_there_or_not_json_says_which() {
        let dir = tempfile::tempdir().unwrap();
        let missing = Dbt::from_manifest(dir.path().join("target/manifest.json"))
            .unwrap_err()
            .to_string();
        assert!(missing.contains("could not read it"), "{missing}");

        let path = written(dir.path(), "not json at all");
        let unparsed = Dbt::from_manifest(path).unwrap_err().to_string();
        assert!(
            unparsed.contains("could not parse it as a dbt manifest"),
            "{unparsed}"
        );

        // json, but not a manifest: the version is what says so
        let path = written(dir.path(), r#"{"nodes": {}}"#);
        let wrong = Dbt::from_manifest(path).unwrap_err().to_string();
        assert!(wrong.contains("could not parse it"), "{wrong}");
    }

    #[test]
    fn two_models_that_would_be_one_asset_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let node = |id: &str| {
            format!(
                r#""{id}": {{"name": "orders", "resource_type": "model",
                   "depends_on": {{"nodes": []}}}}"#
            )
        };
        let path = written(
            dir.path(),
            &format!(
                r#"{{"metadata": {{"dbt_schema_version":
                     "https://schemas.getdbt.com/dbt/manifest/v12.json"}},
                    "nodes": {{{}, {}}}, "sources": {{}}}}"#,
                node("model.a.orders"),
                node("model.b.orders")
            ),
        );
        let err = Dbt::from_manifest(path).unwrap_err().to_string();
        assert!(
            err.contains("model.a.orders and model.b.orders would both be the asset \"orders\""),
            "{err}"
        );
    }
}

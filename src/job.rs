use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use serde_json::{Value, json};

use crate::error::Error;
use crate::graph;
use crate::model::Overlap;
use crate::op::Op;

/// a validated dag of ops, built via [`Job::builder`].
#[derive(Clone, Debug)]
pub struct Job {
    name: String,
    description: Option<String>,
    ops: Vec<Op>,
    order: Vec<String>,
    max_parallel: Option<usize>,
    overlap: Overlap,
    fresh_within: Option<Duration>,
    // every op's declared params schema merged into one, computed at build so
    // a disagreement between two ops is a build error rather than a summary
    // that quietly picks a winner
    params_schema: Option<Value>,
    // dep names satisfied from outside the job: no ops, absent from `order`,
    // seeded at launch with the value declared here — null for an asset
    // source, `[]` for the partition keys a partitioned asset fans out over,
    // which only a build plan can work out. empty for every user-built job.
    external: Vec<(String, Value)>,
}

impl Job {
    pub fn builder(name: impl Into<String>) -> JobBuilder {
        JobBuilder {
            name: name.into(),
            description: None,
            ops: Vec::new(),
            instances: Vec::new(),
            max_parallel: None,
            overlap: Overlap::default(),
            fresh_within: None,
            error: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn op(&self, name: &str) -> Option<&Op> {
        self.ops.iter().find(|o| o.name() == name)
    }

    pub fn max_parallel(&self) -> Option<usize> {
        self.max_parallel
    }

    pub fn overlap(&self) -> Overlap {
        self.overlap
    }

    /// how old this job's latest success may get before it counts as late,
    /// from [`JobBuilder::fresh_within`]. `None` leaves the cron-derived
    /// `overdue` heuristic in charge.
    pub fn fresh_within(&self) -> Option<Duration> {
        self.fresh_within
    }

    /// one object schema for the whole job's params: the
    /// [schemas](crate::Op::params_schema) its ops declared, merged. `None`
    /// when no op declared one, which is every job that has not opted in.
    ///
    /// a launch's params go to every op that will run, so the fields the
    /// launchpad should list are the union of what the ops describe — merged
    /// once at build rather than per request, since a disagreement between two
    /// ops is a mistake to report, not a request to answer.
    ///
    /// this describes params; it never judges them. `params_error` is still
    /// the only thing that refuses a launch.
    pub fn params_schema(&self) -> Option<&Value> {
        self.params_schema.as_ref()
    }

    /// the first op that refuses `params`, with its reason — the same check a
    /// launch runs, minus the store and the run. `None` means every op that
    /// declared [`Op::params`](crate::Op::params) accepts them.
    pub fn params_error(&self, params: &Value) -> Option<(String, String)> {
        self.ops.iter().find_map(|op| {
            op.validate_params(params)
                .err()
                .map(|reason| (op.name().to_string(), reason))
        })
    }

    pub(crate) fn order(&self) -> &[String] {
        &self.order
    }

    pub(crate) fn is_external(&self, name: &str) -> bool {
        self.external.iter().any(|(n, _)| n == name)
    }

    /// what a launch seeds the job's external names with, before any subset
    /// or resume seeds of its own.
    pub(crate) fn external_seeds(&self) -> HashMap<String, Value> {
        self.external.iter().cloned().collect()
    }

    pub(crate) fn dep_pairs(&self) -> Vec<(String, Vec<String>)> {
        self.ops
            .iter()
            .map(|o| (o.name().to_string(), o.deps().to_vec()))
            .collect()
    }

    /// build a job whose ops may depend on `external` names that are not ops:
    /// validation treats them as pre-satisfied roots, absent from the topo order.
    /// this is how the assets job is lowered.
    pub(crate) fn assemble(
        name: impl Into<String>,
        description: Option<String>,
        ops: Vec<Op>,
        external: Vec<(String, Value)>,
    ) -> Result<Job, Error> {
        let name = name.into();
        let mut pairs: Vec<(String, Vec<String>)> = external
            .iter()
            .map(|(n, _)| (n.clone(), Vec::new()))
            .collect();
        pairs.extend(
            ops.iter()
                .map(|o| (o.name().to_string(), o.deps().to_vec())),
        );
        let order = graph::topo_order(&pairs)
            .map_err(|e| Error::Graph(format!("job {name}: {e}")))?
            .into_iter()
            .filter(|n| !external.iter().any(|(e, _)| e == n))
            .collect();
        validate_mapped(&name, &ops)?;
        validate_isolated(&name, &ops)?;
        let params_schema = merge_params_schemas(&name, &ops)?;
        Ok(Job {
            name,
            description,
            ops,
            order,
            max_parallel: None,
            overlap: Overlap::default(),
            fresh_within: None,
            params_schema,
            external,
        })
    }
}

/// merge one op's named sub-map of a params schema into `into`, refusing a
/// name two ops give different shapes.
///
/// that clash is the one thing a merge cannot paper over: picking a winner
/// would describe a field in terms half the job disagrees with, and the point
/// of the schema is that the description is trustworthy. everything else — a
/// name only one op knows, or two ops agreeing — merges silently.
fn merge_named<'a>(
    job: &str,
    op: &'a str,
    key: &str,
    what: &str,
    schema: &'a serde_json::Map<String, Value>,
    into: &mut BTreeMap<&'a str, (&'a str, &'a Value)>,
) -> Result<(), Error> {
    let Some(from) = schema.get(key) else {
        return Ok(());
    };
    let Some(from) = from.as_object() else {
        return Err(Error::Graph(format!(
            "job {job}: op {op} declares a params schema whose {key} is not an object"
        )));
    };
    for (name, shape) in from {
        match into.get(name.as_str()) {
            Some(&(owner, seen)) if seen != shape => {
                return Err(Error::Graph(format!(
                    "job {job}: ops {owner} and {op} both declare the params schema {what} \
                     {name}, with different shapes"
                )));
            }
            Some(_) => {}
            None => {
                into.insert(name, (op, shape));
            }
        }
    }
    Ok(())
}

/// every op's [declared schema](Op::params_schema) merged into one object
/// schema for the job; `None` when none declared one.
///
/// only `properties`, `required` and `$defs`/`definitions` are read. the last
/// two are what a derived schema puts nested types under and points `$ref` at,
/// so dropping them would leave dangling references in the merged result;
/// both spellings are carried, each under its own key, since a `$ref` names one
/// or the other and not both.
fn merge_params_schemas(job: &str, ops: &[Op]) -> Result<Option<Value>, Error> {
    let mut properties = BTreeMap::new();
    let mut defs = BTreeMap::new();
    let mut definitions = BTreeMap::new();
    let mut required: BTreeSet<&str> = BTreeSet::new();
    let mut declared = false;
    for op in ops {
        let Some(schema) = op.declared_params_schema() else {
            continue;
        };
        let Some(schema) = schema.as_object() else {
            return Err(Error::Graph(format!(
                "job {job}: op {} declares a params schema that is not a json object",
                op.name()
            )));
        };
        declared = true;
        let name = op.name();
        merge_named(job, name, "properties", "property", schema, &mut properties)?;
        merge_named(job, name, "$defs", "$defs entry", schema, &mut defs)?;
        merge_named(
            job,
            name,
            "definitions",
            "definitions entry",
            schema,
            &mut definitions,
        )?;
        match schema.get("required") {
            None => {}
            Some(Value::Array(names)) => {
                for n in names {
                    let Some(n) = n.as_str() else {
                        return Err(Error::Graph(format!(
                            "job {job}: op {name} declares a params schema whose required list \
                             holds something that is not a field name"
                        )));
                    };
                    required.insert(n);
                }
            }
            Some(_) => {
                return Err(Error::Graph(format!(
                    "job {job}: op {name} declares a params schema whose required is not an array"
                )));
            }
        }
    }
    if !declared {
        return Ok(None);
    }
    let owned = |map: BTreeMap<&str, (&str, &Value)>| -> Value {
        Value::Object(
            map.into_iter()
                .map(|(name, (_, shape))| (name.to_string(), shape.clone()))
                .collect(),
        )
    };
    let mut merged = serde_json::Map::new();
    merged.insert("type".into(), json!("object"));
    merged.insert("properties".into(), owned(properties));
    // a field one op requires is required of the launch, since the params go
    // to every op that runs
    if !required.is_empty() {
        merged.insert("required".into(), json!(required));
    }
    if !defs.is_empty() {
        merged.insert("$defs".into(), owned(defs));
    }
    if !definitions.is_empty() {
        merged.insert("definitions".into(), owned(definitions));
    }
    Ok(Some(Value::Object(merged)))
}

// a mapped op needs exactly one array to expand over, and its instances are
// ordinary ops that cannot fan out again — one level, deliberately
fn validate_mapped(job: &str, ops: &[Op]) -> Result<(), Error> {
    for op in ops {
        match (op.is_mapped(), op.mapped_over()) {
            (true, None) => {
                return Err(Error::Graph(format!(
                    "job {job}: op {} is mapped but names no dep to map over; add .over(dep)",
                    op.name()
                )));
            }
            (false, Some(dep)) => {
                return Err(Error::Graph(format!(
                    "job {job}: op {} declares .over({dep}) without being an Op::mapped",
                    op.name()
                )));
            }
            (true, Some(dep)) => {
                if ops.iter().any(|o| o.name() == dep && o.is_mapped()) {
                    return Err(Error::Graph(format!(
                        "job {job}: op {} maps over {dep}, which is itself mapped; \
                         fan-out does not nest",
                        op.name()
                    )));
                }
            }
            (false, None) => {}
        }
    }
    Ok(())
}

// what an isolated op may and may not also be. every one of these is refused
// here rather than at run time, because each is a promise hestan would
// otherwise have to break quietly: a limit that cannot be applied, a platform
// with no way to kill a child, an input the child has no way to read.
fn validate_isolated(job: &str, ops: &[Op]) -> Result<(), Error> {
    for op in ops {
        let name = op.name();
        if !op.is_isolated() {
            continue;
        }
        // the platform check first: off unix none of the rest matters
        #[cfg(not(unix))]
        return Err(Error::Graph(format!(
            "job {job}: op {name} is .isolated(), which hestan supports on unix only \
             (this is {}) — an isolation guarantee that quietly is not one is worse \
             than no isolation",
            std::env::consts::OS
        )));
        #[cfg(unix)]
        if op.is_mapped() {
            return Err(Error::Graph(format!(
                "job {job}: op {name} is both mapped and .isolated(); a fan-out instance's \
                 element is the one thing a child cannot read out of the store"
            )));
        }
    }
    Ok(())
}

/// a reusable unit of ops, instantiated into jobs by name with
/// [`JobBuilder::graph`]. build one with [`Graph::builder`].
///
/// a graph is a build-time thing and nothing else: `JobBuilder::build`
/// flattens every instance into ordinary ops named `{instance}.{inner}`, so
/// runs, resume, fan-out, assets and the ui never learn that a graph existed.
/// two instances of one graph in a job are two independent sets of ops, which
/// is what the instance name is for.
///
/// ```no_run
/// # use hestan::{Graph, Job, Op};
/// # use serde_json::json;
/// # fn main() -> Result<(), hestan::Error> {
/// let clean = Graph::builder("clean")
///     .op(Op::new("parse", |_| async { Ok(json!(null)) }))
///     .op(Op::new("dedupe", |_| async { Ok(json!(null)) }).after(["parse"]))
///     .input("parse")     // inner ops that receive the instance's deps
///     .output("dedupe")   // the inner op that supplies the instance output
///     .build()?;
///
/// Job::builder("nightly")
///     .op(Op::new("fetch", |_| async { Ok(json!(null)) }))
///     .graph("clean_a", &clean)
///     .after(["fetch"])
///     .op(Op::new("load", |_| async { Ok(json!(null)) }).after(["clean_a"]))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Graph {
    name: String,
    ops: Vec<Op>,
    instances: Vec<Instance>,
    inputs: Vec<String>,
    output: String,
}

// one use of a graph: the name it goes by in the enclosing scope, the graph
// itself, and what it waits on there
#[derive(Clone, Debug)]
struct Instance {
    name: String,
    graph: Graph,
    deps: Vec<String>,
}

impl Graph {
    pub fn builder(name: impl Into<String>) -> GraphBuilder {
        GraphBuilder {
            name: name.into(),
            ops: Vec::new(),
            instances: Vec::new(),
            inputs: Vec::new(),
            output: None,
            error: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct GraphBuilder {
    name: String,
    ops: Vec<Op>,
    instances: Vec<Instance>,
    inputs: Vec<String>,
    output: Option<String>,
    // a misuse that has no Result to return; build reports it
    error: Option<String>,
}

impl GraphBuilder {
    pub fn op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    /// nest another graph inside this one. nesting is fine — it is all
    /// flattened at job build — and works exactly as it does on a job:
    /// follow it with [`after`](Self::after).
    pub fn graph(mut self, name: impl Into<String>, graph: &Graph) -> Self {
        self.instances.push(Instance {
            name: name.into(),
            graph: graph.clone(),
            deps: Vec::new(),
        });
        self
    }

    /// what the nested instance just added waits on, in this graph's names.
    pub fn after<I>(mut self, deps: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        after_instance(
            &mut self.instances,
            &mut self.error,
            deps.into_iter().map(Into::into),
        );
        self
    }

    /// an inner op (or nested instance) that receives whatever the instance
    /// itself waits on. repeatable; at least one is required, since a graph
    /// with no way in can only ever be wired as a root.
    pub fn input(mut self, op: impl Into<String>) -> Self {
        self.inputs.push(op.into());
        self
    }

    /// the one inner op (or nested instance) whose output *is* the instance's
    /// output — what anything depending on the instance name receives.
    /// required, and the last call wins.
    pub fn output(mut self, op: impl Into<String>) -> Self {
        self.output = Some(op.into());
        self
    }

    /// validates the graph on its own terms: names free of the `.` that
    /// separates an instance from its inner ops, no duplicates, no cycles,
    /// every inner dep naming something inside, and declared inputs and an
    /// output that exist.
    pub fn build(self) -> Result<Graph, Error> {
        let g = format!("graph {}", self.name);
        if let Some(e) = self.error {
            return Err(Error::Graph(format!("{g}: {e}")));
        }
        // ops and nested instances share one namespace: both are things an
        // inner op can name
        let mut names: Vec<&str> = Vec::new();
        for name in self
            .ops
            .iter()
            .map(Op::name)
            .chain(self.instances.iter().map(|i| i.name.as_str()))
        {
            if name.contains('.') {
                return Err(Error::Graph(format!(
                    "{g}: name {name} contains a dot, which separates an instance from its inner ops"
                )));
            }
            if names.contains(&name) {
                return Err(Error::Graph(format!("{g}: duplicate name {name}")));
            }
            names.push(name);
        }
        // a graph's only wiring to the outside is its declared inputs, so an
        // inner dep that names nothing inside is a mistake, not a forward
        // reference to whatever job it lands in
        let outward = |owner: &str, dep: &str| {
            Error::Graph(format!(
                "{g}: {owner} depends on {dep}, which is not in this graph"
            ))
        };
        for op in &self.ops {
            for dep in op.deps() {
                if !names.contains(&dep.as_str()) {
                    return Err(outward(&format!("op {}", op.name()), dep));
                }
            }
        }
        for inst in &self.instances {
            for dep in &inst.deps {
                if !names.contains(&dep.as_str()) {
                    return Err(outward(&format!("instance {}", inst.name), dep));
                }
            }
        }
        let named = |kind: &str, n: &str| {
            if n.contains('.') {
                return Err(Error::Graph(format!(
                    "{g}: {kind} {n} contains a dot, which names an inner op of an instance"
                )));
            }
            if !names.contains(&n) {
                return Err(Error::Graph(format!(
                    "{g}: {kind} {n} is not in this graph"
                )));
            }
            Ok(())
        };
        if self.inputs.is_empty() {
            return Err(Error::Graph(format!(
                "{g}: no input declared; name the inner ops that receive the instance's deps"
            )));
        }
        for input in &self.inputs {
            named("input", input)?;
        }
        let Some(output) = self.output else {
            return Err(Error::Graph(format!(
                "{g}: no output declared; exactly one inner op supplies the instance output"
            )));
        };
        named("output", &output)?;
        // the inner dag, so a cycle is named in the graph's own terms rather
        // than in flattened ones
        let mut pairs: Vec<(String, Vec<String>)> = self
            .ops
            .iter()
            .map(|o| (o.name().to_string(), o.deps().to_vec()))
            .collect();
        pairs.extend(
            self.instances
                .iter()
                .map(|i| (i.name.clone(), i.deps.clone())),
        );
        graph::topo_order(&pairs).map_err(|e| Error::Graph(format!("{g}: {e}")))?;
        Ok(Graph {
            name: self.name,
            ops: self.ops,
            instances: self.instances,
            inputs: self.inputs,
            output,
        })
    }
}

// `.after` attaches to the instance just declared; anywhere else it is a
// misuse worth naming, since an op's own deps belong on the op
fn after_instance(
    instances: &mut [Instance],
    error: &mut Option<String>,
    deps: impl Iterator<Item = String>,
) {
    match instances.last_mut() {
        Some(inst) => inst.deps.extend(deps),
        None if error.is_none() => {
            *error = Some(
                "after(..) does not follow a graph instance; an op's own deps go on the op"
                    .to_string(),
            );
        }
        None => {}
    }
}

/// the op that actually carries an instance's output, following a declared
/// output that names a nested instance down to a real op.
fn output_op(prefix: &str, graph: &Graph) -> String {
    let name = format!("{prefix}.{}", graph.output);
    match graph.instances.iter().find(|i| i.name == graph.output) {
        Some(inner) => output_op(&name, &inner.graph),
        None => name,
    }
}

/// an op as it appears after flattening: renamed, with every dep name it holds
/// rewritten through `wiring`, and `extra` — whatever the instance itself
/// waits on — appended because this op was declared an input. names `wiring`
/// does not cover are left alone, so an unknown dep is still the topo sort's
/// to report, in the same words as ever.
fn rebind(op: &Op, name: String, wiring: &HashMap<String, String>, extra: &[String]) -> Op {
    let look = |dep: &str| wiring.get(dep).cloned().unwrap_or_else(|| dep.to_string());
    let mut deps = Vec::with_capacity(op.deps().len() + extra.len());
    // a rewritten dep keeps the name the body knows it by, so an op reads
    // `ctx.input("parse")` inside a graph rather than `ctx.input("clean_a.parse")`
    let mut aliases = Vec::new();
    for dep in op.deps() {
        let flat = look(dep);
        if &flat != dep {
            aliases.push((flat.clone(), dep.clone()));
        }
        deps.push(flat);
    }
    deps.extend(extra.iter().cloned());
    let over = op.mapped_over().map(look);
    op.clone().rebound(name, deps, over, aliases)
}

/// expand one instance into ordinary ops, appending them to `out`. inner ops
/// are renamed `{prefix}.{inner}` with their deps rewritten to match; the ones
/// the graph declared as inputs additionally wait on `outer`. a nested
/// instance is the same transformation one level down, which is the whole
/// reason the runtime never learns about any of this.
fn expand(
    job: &str,
    prefix: &str,
    graph: &Graph,
    outer: &[String],
    path: &mut Vec<String>,
    out: &mut Vec<Op>,
) -> Result<(), Error> {
    // nesting is a dag by construction — a graph can only contain graphs that
    // were built before it — but flattening something that did contain itself
    // would not terminate, so say so instead of finding out
    if path.contains(&graph.name) {
        return Err(Error::Graph(format!(
            "job {job}: graph {} contains itself ({})",
            graph.name,
            path.join(" -> ")
        )));
    }
    path.push(graph.name.clone());
    // every name an inner op may depend on, and the flattened op it becomes
    let mut wiring: HashMap<String, String> = graph
        .ops
        .iter()
        .map(|o| (o.name().to_string(), format!("{prefix}.{}", o.name())))
        .collect();
    for inst in &graph.instances {
        let at = format!("{prefix}.{}", inst.name);
        wiring.insert(inst.name.clone(), output_op(&at, &inst.graph));
    }
    for op in &graph.ops {
        let extra: &[String] = if graph.inputs.iter().any(|i| i == op.name()) {
            outer
        } else {
            &[]
        };
        out.push(rebind(
            op,
            format!("{prefix}.{}", op.name()),
            &wiring,
            extra,
        ));
    }
    for inst in &graph.instances {
        let mut inner: Vec<String> = inst
            .deps
            .iter()
            .map(|d| wiring.get(d).cloned().unwrap_or_else(|| d.clone()))
            .collect();
        // an input that names a nested instance passes the deps on down
        if graph.inputs.contains(&inst.name) {
            inner.extend(outer.iter().cloned());
        }
        let at = format!("{prefix}.{}", inst.name);
        expand(job, &at, &inst.graph, &inner, path, out)?;
    }
    path.pop();
    Ok(())
}

/// flatten a job's graph instances into ordinary ops. jobs without any — every
/// job before graphs existed — come through untouched.
fn flatten(job: &str, ops: Vec<Op>, instances: Vec<Instance>) -> Result<Vec<Op>, Error> {
    if instances.is_empty() {
        return Ok(ops);
    }
    // two instances of one graph is the point of the instance name; two
    // instances *sharing* one is the mistake it guards against
    let mut taken: Vec<String> = ops.iter().map(|o| o.name().to_string()).collect();
    for inst in &instances {
        if taken.contains(&inst.name) {
            return Err(Error::Graph(format!(
                "job {job}: graph instance {} collides with another op or instance of that name",
                inst.name
            )));
        }
        taken.push(inst.name.clone());
    }
    // an op depending on the instance name means the op the graph declared as
    // its output
    let wiring: HashMap<String, String> = instances
        .iter()
        .map(|i| (i.name.clone(), output_op(&i.name, &i.graph)))
        .collect();
    let mut out: Vec<Op> = ops
        .iter()
        .map(|o| rebind(o, o.name().to_string(), &wiring, &[]))
        .collect();
    for inst in &instances {
        let outer: Vec<String> = inst
            .deps
            .iter()
            .map(|d| wiring.get(d).cloned().unwrap_or_else(|| d.clone()))
            .collect();
        expand(
            job,
            &inst.name,
            &inst.graph,
            &outer,
            &mut Vec::new(),
            &mut out,
        )?;
    }
    Ok(out)
}

pub struct JobBuilder {
    name: String,
    description: Option<String>,
    ops: Vec<Op>,
    instances: Vec<Instance>,
    max_parallel: Option<usize>,
    overlap: Overlap,
    fresh_within: Option<Duration>,
    error: Option<String>,
}

impl JobBuilder {
    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.description = Some(d.into());
        self
    }

    pub fn op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    /// instantiate a reusable [`Graph`] under `name`. its inner ops join the
    /// job as `{name}.{inner}`, and an op that depends on `name` is wired to
    /// whichever inner op the graph declared as its output. follow it with
    /// [`after`](Self::after) to say what the instance waits on.
    pub fn graph(mut self, name: impl Into<String>, graph: &Graph) -> Self {
        self.instances.push(Instance {
            name: name.into(),
            graph: graph.clone(),
            deps: Vec::new(),
        });
        self
    }

    /// what the graph instance just added waits on: the job-level deps its
    /// declared input ops additionally gain. only meaningful straight after
    /// [`graph`](Self::graph) — anywhere else it is a build error, since an
    /// op's own deps belong on the op ([`Op::after`]).
    pub fn after<I>(mut self, deps: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        after_instance(
            &mut self.instances,
            &mut self.error,
            deps.into_iter().map(Into::into),
        );
        self
    }

    /// cap how many ops of this job run at once; values below 1 mean 1.
    pub fn max_parallel(mut self, n: usize) -> Self {
        self.max_parallel = Some(n.max(1));
        self
    }

    /// what a scheduled fire does while a run of this job is still active.
    /// skip is the default; manual launches are never gated.
    pub fn overlap(mut self, o: Overlap) -> Self {
        self.overlap = o;
        self
    }

    /// declare how old this job's latest success may get before the job is
    /// late: `fresh_within(Duration::from_secs(86_400))` says a successful run
    /// every day. a declared policy takes over from the cron-derived `overdue`
    /// heuristic entirely — see [freshness](../docs/freshness.md) — and is what
    /// `Hestan::on_late` alerts on.
    pub fn fresh_within(mut self, d: Duration) -> Self {
        self.fresh_within = Some(d);
        self
    }

    /// flattens any [`graph`](Self::graph) instances into ordinary ops, then
    /// validates the dag; fails on duplicate ops, unknown deps, or cycles.
    pub fn build(self) -> Result<Job, Error> {
        if let Some(e) = self.error {
            return Err(Error::Graph(format!("job {}: {e}", self.name)));
        }
        let ops = flatten(&self.name, self.ops, self.instances)?;
        let pairs: Vec<_> = ops
            .iter()
            .map(|o| (o.name().to_string(), o.deps().to_vec()))
            .collect();
        let order = graph::topo_order(&pairs)
            .map_err(|e| Error::Graph(format!("job {}: {e}", self.name)))?;
        validate_mapped(&self.name, &ops)?;
        validate_isolated(&self.name, &ops)?;
        let params_schema = merge_params_schemas(&self.name, &ops)?;
        Ok(Job {
            name: self.name,
            description: self.description,
            ops,
            order,
            max_parallel: self.max_parallel,
            overlap: self.overlap,
            fresh_within: self.fresh_within,
            params_schema,
            external: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn op(name: &str) -> Op {
        Op::new(name, |_| async { Ok(json!(null)) })
    }

    // parse -> dedupe, in and out at either end
    fn clean() -> Graph {
        Graph::builder("clean")
            .op(op("parse"))
            .op(op("dedupe").after(["parse"]))
            .input("parse")
            .output("dedupe")
            .build()
            .unwrap()
    }

    fn names(job: &Job) -> Vec<&str> {
        job.ops().iter().map(Op::name).collect()
    }

    fn deps<'a>(job: &'a Job, op: &str) -> &'a [String] {
        job.op(op).unwrap_or_else(|| panic!("no op {op}")).deps()
    }

    fn build_err(b: JobBuilder) -> String {
        match b.build() {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a build error"),
        }
    }

    // a fan-out instance is handed one element of an array, and an element is
    // the one input that is nowhere a child process could read it
    #[test]
    fn an_isolated_op_may_not_also_fan_out() {
        let err = build_err(
            Job::builder("fan").op(op("pages")).op(Op::mapped(
                "fetch",
                |_ctx, page: u32| async move { Ok(json!(page)) },
            )
            .over("pages")
            .isolated()),
        );
        assert!(err.contains("both mapped and .isolated()"), "{err}");

        // and neither restriction touches an ordinary op beside one
        Job::builder("fan")
            .op(op("pages"))
            .op(Op::mapped("fetch", |_ctx, page: u32| async move { Ok(json!(page)) }).over("pages"))
            .op(op("upload").isolated())
            .build()
            .unwrap();
    }

    #[test]
    fn an_instance_flattens_into_prefixed_ops_wired_at_both_ends() {
        let job = Job::builder("nightly")
            .op(op("fetch"))
            .graph("clean_a", &clean())
            .after(["fetch"])
            .op(op("load").after(["clean_a"]))
            .build()
            .unwrap();

        assert_eq!(
            names(&job),
            ["fetch", "load", "clean_a.parse", "clean_a.dedupe"]
        );
        // the declared input gains the instance's own deps
        assert_eq!(deps(&job, "clean_a.parse"), ["fetch"]);
        // inner deps are prefixed too
        assert_eq!(deps(&job, "clean_a.dedupe"), ["clean_a.parse"]);
        // and the outside depends on the declared output, not on the instance
        assert_eq!(deps(&job, "load"), ["clean_a.dedupe"]);
        // topo order still holds over the flattened graph
        assert_eq!(
            job.order(),
            ["fetch", "clean_a.parse", "clean_a.dedupe", "load"]
        );
    }

    #[test]
    fn two_instances_of_one_graph_stay_independent() {
        let clean = clean();
        let job = Job::builder("nightly")
            .op(op("fetch_a"))
            .op(op("fetch_b"))
            .graph("clean_a", &clean)
            .after(["fetch_a"])
            .graph("clean_b", &clean)
            .after(["fetch_b"])
            .op(op("merge").after(["clean_a", "clean_b"]))
            .build()
            .unwrap();

        assert_eq!(deps(&job, "clean_a.parse"), ["fetch_a"]);
        assert_eq!(deps(&job, "clean_b.parse"), ["fetch_b"]);
        assert_eq!(deps(&job, "clean_a.dedupe"), ["clean_a.parse"]);
        assert_eq!(deps(&job, "clean_b.dedupe"), ["clean_b.parse"]);
        assert_eq!(deps(&job, "merge"), ["clean_a.dedupe", "clean_b.dedupe"]);
    }

    #[test]
    fn a_graph_inside_a_graph_flattens_all_the_way_down() {
        let inner = clean();
        let outer = Graph::builder("stage")
            .op(op("pre"))
            .graph("scrub", &inner)
            .after(["pre"])
            .op(op("post").after(["scrub"]))
            .input("pre")
            .output("post")
            .build()
            .unwrap();
        let job = Job::builder("nightly")
            .op(op("fetch"))
            .graph("s1", &outer)
            .after(["fetch"])
            .op(op("load").after(["s1"]))
            .build()
            .unwrap();

        assert_eq!(
            names(&job),
            [
                "fetch",
                "load",
                "s1.pre",
                "s1.post",
                "s1.scrub.parse",
                "s1.scrub.dedupe",
            ]
        );
        assert_eq!(deps(&job, "s1.pre"), ["fetch"]);
        assert_eq!(deps(&job, "s1.scrub.parse"), ["s1.pre"]);
        assert_eq!(deps(&job, "s1.scrub.dedupe"), ["s1.scrub.parse"]);
        // post depends on the nested instance, so on its declared output
        assert_eq!(deps(&job, "s1.post"), ["s1.scrub.dedupe"]);
        assert_eq!(deps(&job, "load"), ["s1.post"]);
    }

    // input and output may name a nested instance, which resolves through it
    #[test]
    fn declared_io_may_name_a_nested_instance() {
        let inner = clean();
        let outer = Graph::builder("wrap")
            .graph("core", &inner)
            .input("core")
            .output("core")
            .build()
            .unwrap();
        let job = Job::builder("nightly")
            .op(op("fetch"))
            .graph("w", &outer)
            .after(["fetch"])
            .op(op("load").after(["w"]))
            .build()
            .unwrap();

        assert_eq!(deps(&job, "w.core.parse"), ["fetch"]);
        assert_eq!(deps(&job, "load"), ["w.core.dedupe"]);
    }

    // a mapped op inside a graph fans out over a prefixed dep like any other
    #[test]
    fn fan_out_inside_a_graph_keeps_its_mapped_dep() {
        let paged = Graph::builder("paged")
            .op(op("pages"))
            .op(
                Op::mapped("fetch", |_ctx: crate::op::OpCtx, page: u32| async move {
                    Ok(json!(page))
                })
                .over("pages"),
            )
            .input("pages")
            .output("fetch")
            .build()
            .unwrap();
        let job = Job::builder("nightly")
            .op(op("config"))
            .graph("p", &paged)
            .after(["config"])
            .build()
            .unwrap();

        assert_eq!(job.op("p.fetch").unwrap().mapped_over(), Some("p.pages"));
        assert_eq!(deps(&job, "p.fetch"), ["p.pages"]);
        assert_eq!(deps(&job, "p.pages"), ["config"]);
    }

    #[test]
    fn duplicate_instance_names_and_op_collisions_are_rejected() {
        let clean = clean();
        let err = build_err(
            Job::builder("nightly")
                .graph("clean_a", &clean)
                .graph("clean_a", &clean),
        );
        assert!(err.contains("graph instance clean_a collides"), "{err}");

        let err = build_err(
            Job::builder("nightly")
                .op(op("clean_a"))
                .graph("clean_a", &clean),
        );
        assert!(err.contains("collides"), "{err}");
    }

    #[test]
    fn a_graph_needs_a_real_input_and_output() {
        let missing_output = Graph::builder("g")
            .op(op("only"))
            .input("only")
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            missing_output.contains("no output declared"),
            "{missing_output}"
        );

        let missing_input = Graph::builder("g")
            .op(op("only"))
            .output("only")
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            missing_input.contains("no input declared"),
            "{missing_input}"
        );

        let unknown = Graph::builder("g")
            .op(op("only"))
            .input("ghost")
            .output("only")
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            unknown.contains("input ghost is not in this graph"),
            "{unknown}"
        );

        let unknown = Graph::builder("g")
            .op(op("only"))
            .input("only")
            .output("ghost")
            .build()
            .unwrap_err()
            .to_string();
        assert!(
            unknown.contains("output ghost is not in this graph"),
            "{unknown}"
        );
    }

    #[test]
    fn inner_names_may_not_carry_the_separator_or_reach_outward() {
        let dotted = Graph::builder("g")
            .op(op("a.b"))
            .input("a.b")
            .output("a.b")
            .build()
            .unwrap_err()
            .to_string();
        assert!(dotted.contains("contains a dot"), "{dotted}");

        let dotted = Graph::builder("g")
            .op(op("a"))
            .input("a")
            .output("a.b")
            .build()
            .unwrap_err()
            .to_string();
        assert!(dotted.contains("output a.b contains a dot"), "{dotted}");

        // a graph's only way in is its declared inputs
        let outward = Graph::builder("g")
            .op(op("a").after(["fetch"]))
            .input("a")
            .output("a")
            .build()
            .unwrap_err()
            .to_string();
        assert!(outward.contains("op a depends on fetch"), "{outward}");

        let dup = Graph::builder("g")
            .op(op("a"))
            .op(op("a"))
            .input("a")
            .output("a")
            .build()
            .unwrap_err()
            .to_string();
        assert!(dup.contains("duplicate name a"), "{dup}");

        let cycle = Graph::builder("g")
            .op(op("a").after(["b"]))
            .op(op("b").after(["a"]))
            .input("a")
            .output("b")
            .build()
            .unwrap_err()
            .to_string();
        assert!(cycle.contains("cycle"), "{cycle}");
    }

    #[test]
    fn after_without_an_instance_is_a_build_error() {
        let err = build_err(Job::builder("nightly").op(op("a")).after(["a"]));
        assert!(err.contains("does not follow a graph instance"), "{err}");

        let err = Graph::builder("g")
            .op(op("a"))
            .after(["a"])
            .input("a")
            .output("a")
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not follow a graph instance"), "{err}");
    }

    // the launch's params go to every op that runs, so the job's schema is the
    // union of what its ops describe
    #[test]
    fn declared_params_schemas_merge_into_one_object_schema() {
        let job = Job::builder("report")
            .op(op("fetch").params_schema(json!({
                "type": "object",
                "properties": { "region": { "type": "string" } },
                "required": ["region"],
                "$defs": { "Region": { "type": "string" } }
            })))
            .op(op("render").after(["fetch"]).params_schema(json!({
                "type": "object",
                // region again, identically: agreement is not a conflict
                "properties": {
                    "region": { "type": "string" },
                    "days": { "type": "integer", "description": "how far back" }
                }
            })))
            .op(op("notify").after(["render"]))
            .build()
            .unwrap();

        assert_eq!(
            job.params_schema().unwrap(),
            &json!({
                "type": "object",
                "properties": {
                    "days": { "type": "integer", "description": "how far back" },
                    "region": { "type": "string" }
                },
                "required": ["region"],
                "$defs": { "Region": { "type": "string" } }
            })
        );
        // the ops keep what each of them declared, merged or not
        assert!(job.op("fetch").unwrap().declared_params_schema().is_some());
        assert!(job.op("notify").unwrap().declared_params_schema().is_none());

        // a job nobody described has no schema at all, not an empty one
        let plain = Job::builder("plain").op(op("a")).build().unwrap();
        assert!(plain.params_schema().is_none());
    }

    #[test]
    fn conflicting_params_schemas_fail_the_build_naming_both_ops() {
        let clash = |a: Value, b: Value| {
            build_err(
                Job::builder("report")
                    .op(op("fetch").params_schema(a))
                    .op(op("render").after(["fetch"]).params_schema(b)),
            )
        };

        let err = clash(
            json!({"properties": { "days": { "type": "integer" } }}),
            json!({"properties": { "days": { "type": "string" } }}),
        );
        assert!(
            err.contains("ops fetch and render both declare the params schema property days"),
            "{err}"
        );

        // the same rule over the nested-type maps a $ref points at
        let err = clash(
            json!({"$defs": { "Window": { "type": "integer" } }}),
            json!({"$defs": { "Window": { "type": "string" } }}),
        );
        assert!(err.contains("$defs entry Window"), "{err}");
        let err = clash(
            json!({"definitions": { "Window": { "type": "integer" } }}),
            json!({"definitions": { "Window": { "type": "string" } }}),
        );
        assert!(err.contains("definitions entry Window"), "{err}");

        // and a schema that is not a schema is caught where it is declared
        let err = build_err(Job::builder("report").op(op("fetch").params_schema(json!("days"))));
        assert!(
            err.contains("op fetch declares a params schema that is not"),
            "{err}"
        );
        let err = build_err(
            Job::builder("report").op(op("fetch").params_schema(json!({"properties": []}))),
        );
        assert!(err.contains("whose properties is not an object"), "{err}");
        let err = build_err(
            Job::builder("report").op(op("fetch").params_schema(json!({"required": "days"}))),
        );
        assert!(err.contains("whose required is not an array"), "{err}");
        let err = build_err(
            Job::builder("report").op(op("fetch").params_schema(json!({"required": [7]}))),
        );
        assert!(err.contains("not a field name"), "{err}");
    }

    // a job with no instances must come out exactly as it went in
    #[test]
    fn a_job_without_instances_is_untouched() {
        let job = Job::builder("plain")
            .op(op("a"))
            .op(op("b").after(["a"]))
            .build()
            .unwrap();
        assert_eq!(names(&job), ["a", "b"]);
        assert_eq!(deps(&job, "b"), ["a"]);

        let err = build_err(Job::builder("plain").op(op("a").after(["ghost"])));
        assert!(err.contains("op a depends on unknown op ghost"), "{err}");
    }
}

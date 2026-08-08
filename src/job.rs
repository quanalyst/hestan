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
    // dep names satisfied from outside the job (asset sources): no ops, absent
    // from `order`, seeded null at launch. empty for every user-built job.
    external: Vec<String>,
}

impl Job {
    pub fn builder(name: impl Into<String>) -> JobBuilder {
        JobBuilder {
            name: name.into(),
            description: None,
            ops: Vec::new(),
            max_parallel: None,
            overlap: Overlap::default(),
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

    pub(crate) fn order(&self) -> &[String] {
        &self.order
    }

    pub(crate) fn external(&self) -> &[String] {
        &self.external
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
        external: Vec<String>,
    ) -> Result<Job, Error> {
        let name = name.into();
        let mut pairs: Vec<(String, Vec<String>)> =
            external.iter().map(|n| (n.clone(), Vec::new())).collect();
        pairs.extend(
            ops.iter()
                .map(|o| (o.name().to_string(), o.deps().to_vec())),
        );
        let order = graph::topo_order(&pairs)
            .map_err(|e| Error::Graph(format!("job {name}: {e}")))?
            .into_iter()
            .filter(|n| !external.contains(n))
            .collect();
        Ok(Job {
            name,
            description,
            ops,
            order,
            max_parallel: None,
            overlap: Overlap::default(),
            external,
        })
    }
}

pub struct JobBuilder {
    name: String,
    description: Option<String>,
    ops: Vec<Op>,
    max_parallel: Option<usize>,
    overlap: Overlap,
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

    /// validates the dag; fails on duplicate ops, unknown deps, or cycles.
    pub fn build(self) -> Result<Job, Error> {
        let pairs: Vec<_> = self
            .ops
            .iter()
            .map(|o| (o.name().to_string(), o.deps().to_vec()))
            .collect();
        let order = graph::topo_order(&pairs)
            .map_err(|e| Error::Graph(format!("job {}: {e}", self.name)))?;
        Ok(Job {
            name: self.name,
            description: self.description,
            ops: self.ops,
            order,
            max_parallel: self.max_parallel,
            overlap: self.overlap,
            external: Vec::new(),
        })
    }
}

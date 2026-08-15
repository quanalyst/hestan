use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Error;
use crate::op::InputError;

/// one built resource: the value every op shares, plus the type it was
/// declared as, enough to report what exists without ever showing what is
/// in it.
#[derive(Clone)]
pub(crate) struct Resource {
    pub(crate) type_name: &'static str,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
}

/// every resource one op can read: what this process built at startup, plus
/// whatever the run it belongs to built for itself.
pub(crate) type Resources = Arc<HashMap<String, Resource>>;

/// a resource constructor with its type erased: it returns the built value
/// already boxed, carrying the type name it was declared with.
pub(crate) type ResourceFn = Box<
    dyn FnOnce(
            ResourceCtx,
        )
            -> BoxFuture<'static, Result<Resource, Box<dyn std::error::Error + Send + Sync>>>
        + Send,
>;

/// a run-scoped resource's constructor. `Fn` rather than `FnOnce` because it
/// is called once per run for as long as the process lives, and `Sync` because
/// two runs may be building from it at the same moment.
pub(crate) type RunResourceFn = Arc<
    dyn Fn(
            ResourceCtx,
        )
            -> BoxFuture<'static, Result<Resource, Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + Sync,
>;

/// one declared run-scoped resource.
///
/// the type is here rather than only on the built value because nothing is
/// built until a run starts: this is how the api can say what a deployment
/// holds without launching one to find out.
pub(crate) struct RunResource {
    pub(crate) name: String,
    pub(crate) type_name: &'static str,
    pub(crate) build: RunResourceFn,
}

/// what a runner was told to build per run, shared by every run it starts.
pub(crate) type RunResources = Arc<Vec<RunResource>>;

pub(crate) fn none() -> Resources {
    Arc::new(HashMap::new())
}

/// build one run's own resources over the process's, in declaration order.
///
/// a run-scoped constructor sees everything built before it (the process's
/// own, then the run-scoped ones declared earlier), so a per-run client can
/// lean on a process-wide pool. the first failure is the run's failure, named:
/// an op whose scratch directory does not exist has nothing useful to do.
pub(crate) async fn for_run(
    decls: &RunResources,
    process: &Resources,
    run_id: &str,
) -> Result<RunScoped, Error> {
    if decls.is_empty() {
        return Ok(RunScoped {
            all: process.clone(),
            owned: false,
        });
    }
    let mut built: HashMap<String, Resource> = (**process).clone();
    for decl in decls.iter() {
        let ctx = ResourceCtx {
            name: decl.name.clone(),
            run_id: Some(run_id.to_string()),
            built: built.clone(),
        };
        match (decl.build)(ctx).await {
            Ok(res) => built.insert(decl.name.clone(), res),
            Err(e) => {
                return Err(Error::Resource {
                    name: decl.name.clone(),
                    reason: e.to_string(),
                });
            }
        };
    }
    Ok(RunScoped {
        all: Arc::new(built),
        owned: true,
    })
}

/// what one run's ops read, and what becomes of it when the run ends.
///
/// held by the task driving the run and by nothing else, so **every** way that
/// task ends drops it: a success, a failure, a cancellation, a store that will
/// not record the outcome, and the task simply being dropped when the process
/// stops caring. that last one is why this is a `Drop` rather than a line at
/// the end of the run loop.
pub(crate) struct RunScoped {
    all: Resources,
    /// whether any of `all` belongs to the run. process-wide resources outlive
    /// every run, and dropping the map they are in drops nothing but a
    /// reference count.
    owned: bool,
}

impl RunScoped {
    /// the map an op of this run reads, run-scoped over process-wide.
    pub(crate) fn resources(&self) -> Resources {
        self.all.clone()
    }
}

impl Drop for RunScoped {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        let built = std::mem::replace(&mut self.all, none());
        // a scratch directory removing itself, a client closing a socket, a
        // transaction rolling back: `Drop` is allowed to block and the task
        // driving the run is the one thread that must not, which is the same
        // reason an io manager's work goes to the pool rather than running
        // here. a runtime already shutting down runs nothing new, and drops
        // what it was handed instead: still off this stack, still dropped.
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => drop(rt.spawn_blocking(move || drop(built))),
            // no runtime to hand it to, so here is the only place left. this
            // is what would happen without any of this
            Err(_) => drop(built),
        }
    }
}

/// look one resource up, distinguishing "there is no such resource" from
/// "there is, and it is something else".
pub(crate) fn lookup<T: Any + Send + Sync>(
    built: &HashMap<String, Resource>,
    name: &str,
) -> Result<Arc<T>, InputError> {
    let Some(res) = built.get(name) else {
        return Err(InputError::NoResource(name.to_string()));
    };
    res.value
        .clone()
        .downcast::<T>()
        .map_err(|_| InputError::ResourceType {
            name: name.to_string(),
            got: res.type_name,
            want: std::any::type_name::<T>(),
        })
}

/// what a resource constructor is handed: its own name, and the resources
/// declared before it. resources are built in declaration order, so one can
/// lean on an earlier one (a client on the config it reads), and asking for
/// a later one is [`InputError::NoResource`] rather than a deadlock.
pub struct ResourceCtx {
    name: String,
    run_id: Option<String>,
    built: HashMap<String, Resource>,
}

impl ResourceCtx {
    /// the name this resource is being built under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// the run this value is being built for, on a
    /// [run-scoped](crate::Hestan::run_resource) resource; `None` in a
    /// process-wide constructor, which is built before any run exists.
    ///
    /// the identifier the run log knows the run by, so a scratch directory or
    /// a temporary table named with it can be found afterwards by whoever is
    /// looking at the run.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// a resource declared before this one, as `Arc<T>`.
    pub fn resource<T: Any + Send + Sync>(&self, name: &str) -> Result<Arc<T>, InputError> {
        lookup(&self.built, name)
    }
}

/// build every declared resource, in declaration order. the first one that
/// fails aborts the whole startup naming itself: a process whose api client
/// could not be created has nothing useful to serve.
pub(crate) async fn build(decls: Vec<(String, ResourceFn)>) -> Result<Resources, Error> {
    let mut built: HashMap<String, Resource> = HashMap::new();
    for (name, ctor) in decls {
        if built.contains_key(&name) {
            return Err(Error::Resource {
                name,
                reason: "declared twice".into(),
            });
        }
        let ctx = ResourceCtx {
            name: name.clone(),
            // built before there is a run to belong to, which is the whole
            // difference between the two scopes
            run_id: None,
            built: built.clone(),
        };
        match ctor(ctx).await {
            Ok(res) => {
                built.insert(name, res);
            }
            Err(e) => {
                return Err(Error::Resource {
                    name,
                    reason: e.to_string(),
                });
            }
        }
    }
    Ok(Arc::new(built))
}

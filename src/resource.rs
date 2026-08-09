use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Error;
use crate::op::InputError;

/// one built resource: the value every op shares, plus the type it was
/// declared as — enough to report what exists without ever showing what is
/// in it.
#[derive(Clone)]
pub(crate) struct Resource {
    pub(crate) type_name: &'static str,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
}

/// every resource this process built, shared by every op of every job.
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

pub(crate) fn none() -> Resources {
    Arc::new(HashMap::new())
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
/// lean on an earlier one — a client on the config it reads — and asking for
/// a later one is [`InputError::NoResource`] rather than a deadlock.
pub struct ResourceCtx {
    name: String,
    built: HashMap<String, Resource>,
}

impl ResourceCtx {
    /// the name this resource is being built under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// a resource declared before this one, as `Arc<T>`.
    pub fn resource<T: Any + Send + Sync>(&self, name: &str) -> Result<Arc<T>, InputError> {
        lookup(&self.built, name)
    }
}

/// build every declared resource, in declaration order. the first one that
/// fails aborts the whole startup naming itself — a process whose api client
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

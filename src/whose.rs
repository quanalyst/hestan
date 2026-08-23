//! whose work this is: the namespace that divides a deployment.
//!
//! private, because there is no type here to hand anybody. a namespace is an
//! `Option<String>` on a job, an asset, a schedule and a sensor, and this is
//! the one function that decides whether a declared one is allowed. the
//! concept is written up on the public items that carry it and in
//! `docs/namespaces.md`.

use crate::error::Error;

/// whether `declared` is a namespace this deployment will accept.
///
/// two refusals, and each is about a namespace that would not work as one
/// rather than about taste. `what` and `which` name the thing declaring it, so
/// the build error says which line to go and look at.
pub(crate) fn check_namespace(
    what: &str,
    which: &str,
    declared: Option<&str>,
) -> Result<(), Error> {
    let Some(ns) = declared else {
        return Ok(());
    };
    if ns.trim().is_empty() {
        return Err(Error::Graph(format!(
            "{what} {which}: namespace {ns:?} has no name in it, and a {what} in a namespace \
             with no name is a {what} in no namespace"
        )));
    }
    if ns.trim() != ns {
        return Err(Error::Graph(format!(
            "{what} {which}: namespace {ns:?} starts or ends with a space, which nothing that \
             names it in a url or on a command line can type; declare {:?}",
            ns.trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_declared_is_the_deployment_that_has_no_namespaces() {
        assert!(check_namespace("job", "etl", None).is_ok());
        assert!(check_namespace("job", "etl", Some("finance")).is_ok());
    }

    #[test]
    fn a_namespace_that_could_not_be_named_again_is_refused_at_the_build() {
        for bad in ["", "   ", "\t"] {
            let said = check_namespace("job", "etl", Some(bad))
                .unwrap_err()
                .to_string();
            assert!(said.contains("job etl"), "{said}");
            assert!(said.contains("no name in it"), "{said}");
        }
        // a space nobody can see is a namespace nobody can name in a url, so
        // it is refused with the one that was meant quoted back
        let said = check_namespace("asset", "orders", Some(" finance"))
            .unwrap_err()
            .to_string();
        assert!(said.contains("asset orders"), "{said}");
        assert!(said.contains("declare \"finance\""), "{said}");
    }
}

//! which installation this is, and which build of your code it runs.
//!
//! three identities are tangled in that sentence and this module keeps them
//! apart, because only one of them is hestan's to know:
//!
//! - **hestan's own version**, which it reads out of its own manifest at
//!   compile time, along with the schema version, the features that were
//!   compiled and the platform it was built for. free, and never asked for.
//! - **the application's build**, meaning the git sha, the tag or the image
//!   digest of *your* binary. hestan is a library compiled into it and has no
//!   way to see it, so it is told or it is absent.
//! - **the deployment's name**, meaning this installation as opposed to
//!   staging. also told, also absent by default.
//!
//! the one an operator cares about at 3am is the second, and hestan can only
//! carry what it is given. `docs/deployment.md` is the whole of it.

use serde_json::{Value, json};

/// every feature hestan has, and whether this build compiled it.
///
/// the names are the manifest's, and a case in `tests/docs.rs` reads
/// `[features]` out of `Cargo.toml` and asserts this list is exactly it, so a
/// feature added tomorrow cannot quietly go unreported.
const FEATURES: &[(&str, bool)] = &[
    ("bundled", cfg!(feature = "bundled")),
    ("capture", cfg!(feature = "capture")),
    ("cli", cfg!(feature = "cli")),
    ("dbt", cfg!(feature = "dbt")),
    ("http", cfg!(feature = "http")),
    ("otel", cfg!(feature = "otel")),
    ("parquet", cfg!(feature = "parquet")),
    ("postgres", cfg!(feature = "postgres")),
];

/// what a deployment says it is: a name for this installation and the build of
/// your application running in it.
///
/// ```
/// # use hestan::{Deployment, Hestan};
/// # fn f(app: Hestan) -> Hestan {
/// app.deployment(
///     Deployment::new()
///         .name("prod-eu")
///         // set by your build, however your build sets things: a git sha
///         // baked in with `env!`, an image digest read out of the
///         // environment, a tag your ci wrote down
///         .build(std::env::var("APP_BUILD").unwrap_or_default()),
/// )
/// # }
/// ```
///
/// **both halves are optional and a deployment that declares neither is the
/// ordinary case.** one process on a laptop has nothing to distinguish itself
/// from and no build to name, and it should not have to fill anything in to
/// get a run log.
///
/// **hestan cannot work out the build for you and does not pretend to.** it is
/// a library linked into your binary: the sha it would want is the sha of a
/// repository it is not in, and a version it invented would be worse than the
/// absence. what it does know without being told is on the associated
/// functions below, and every one of them is a compile-time fact.
///
/// where it surfaces: `GET /api/health`, `hestan doctor`, the ui's activity
/// page, and, for the build, the `build` column of every run launched while it
/// was declared. that last one is [recorded, not joined](crate::Run::build): a
/// run log read six months from now still says which build produced each run,
/// rather than answering with whatever is deployed on the day it is read.
///
/// **fields are private and there are accessors**, the pattern
/// [`Owner`](crate::Owner) set: this is a struct callers build, and a struct
/// callers build must be able to gain a field without breaking every literal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deployment {
    name: Option<String>,
    build: Option<String>,
}

impl Deployment {
    /// a deployment that declares nothing, which is what every deployment
    /// that does not call this already has.
    pub fn new() -> Deployment {
        Deployment::default()
    }

    /// what this installation is called: `prod-eu`, `staging`, `analytics`.
    ///
    /// hestan neither parses nor enforces it. it is the word that tells two
    /// run logs apart when somebody is looking at the wrong one.
    ///
    /// **an empty string is an absence**, for the reason below.
    pub fn name(mut self, name: impl Into<String>) -> Deployment {
        self.name = declared(name);
        self
    }

    /// which build of your application this is: a git sha, a tag, an image
    /// digest, a ci build number. whatever your deployment already has.
    ///
    /// **an empty string is an absence**, because
    /// `std::env::var("APP_BUILD").unwrap_or_default()` is how a deployment
    /// that meant to set it and did not gets here, and a build called `""` on
    /// every run row would be worse than no build at all: it reads as an
    /// answer.
    pub fn build(mut self, build: impl Into<String>) -> Deployment {
        self.build = declared(build);
        self
    }

    /// the name, if one was declared.
    pub fn called(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// the build, if one was declared. `None` is hestan not having been told,
    /// which is the only state it can be in about somebody else's build.
    pub fn build_id(&self) -> Option<&str> {
        self.build.as_deref()
    }

    /// whether anything at all was declared.
    pub fn declared(&self) -> bool {
        self.name.is_some() || self.build.is_some()
    }

    /// the version of hestan compiled into this binary.
    ///
    /// free: it is `CARGO_PKG_VERSION` at the moment hestan itself was
    /// compiled. **not the version of your application**, which is the one an
    /// operator is usually after; that is [`build`](Deployment::build) and
    /// hestan has to be told it.
    pub fn hestan_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// the store schema this build reads and writes.
    ///
    /// a database is migrated up to it on open and a database already past it
    /// is refused, so this is what says whether two processes can share a run
    /// log.
    pub fn schema_version() -> u32 {
        crate::store::SCHEMA_VERSION
    }

    /// the features compiled into this build, in name order.
    ///
    /// every one is a `cfg!` and so is settled at compile time. this is the
    /// answer to "why does this deployment not have the postgres store", which
    /// is a question about the binary rather than about the configuration.
    pub fn features() -> Vec<&'static str> {
        FEATURES
            .iter()
            .filter(|(_, on)| *on)
            .map(|(name, _)| *name)
            .collect()
    }

    /// what this build was compiled for, as `os/arch`: `linux/aarch64`,
    /// `macos/x86_64`.
    ///
    /// free, from `std::env::consts`, which are compile-time constants like
    /// everything else here. worth having where a deployment runs mixed hosts:
    /// an [isolated op](crate::Op::isolated) is a unix feature, and a run that
    /// behaved differently on one host than on another is a question that
    /// starts here.
    pub fn platform() -> String {
        format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
    }

    /// whether this build has debug assertions on.
    ///
    /// what `--release` turns off, so it is very nearly "is this a debug
    /// build" and is reported as the fact it actually is rather than as the
    /// inference: a profile that overrides `debug-assertions` makes the two
    /// come apart, and a release build running ten times slower than expected
    /// is the reason anybody looks.
    pub fn debug_assertions() -> bool {
        cfg!(debug_assertions)
    }

    /// the whole of it as one object: what was declared, then what hestan
    /// knows without being told, kept in separate halves on purpose.
    ///
    /// this is the shape `GET /api/health` carries and the ui reads.
    pub(crate) fn describe(&self) -> Value {
        json!({
            // null until a deployment says otherwise, which is not "unnamed":
            // it is nobody having told hestan
            "name": self.name,
            // and null until a deployment says otherwise, which is the only
            // honest answer hestan has about somebody else's build
            "build": self.build,
            // everything below is a compile-time fact about the hestan in this
            // binary, and none of it says anything about your application
            "hestan": {
                "version": Deployment::hestan_version(),
                "schema": Deployment::schema_version(),
                "features": Deployment::features(),
                "platform": Deployment::platform(),
                "debug_assertions": Deployment::debug_assertions(),
            },
        })
    }
}

/// what was actually said, or nothing. an empty string is what a deployment
/// that read an unset environment variable passes in, and it is not a name.
fn declared(value: impl Into<String>) -> Option<String> {
    Some(value.into()).filter(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_deployment_that_declares_nothing_says_so_rather_than_guessing() {
        let d = Deployment::new();
        assert_eq!(d.called(), None);
        assert_eq!(d.build_id(), None);
        assert!(!d.declared());
        let said = d.describe();
        assert!(said["name"].is_null());
        assert!(said["build"].is_null());
        // and what it knows without being told is there either way
        assert_eq!(said["hestan"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(said["hestan"]["schema"], crate::store::SCHEMA_VERSION);
    }

    #[test]
    fn a_declared_name_and_build_reach_the_description_unchanged() {
        let d = Deployment::new().name("prod-eu").build("9f2c1ab");
        assert_eq!(d.called(), Some("prod-eu"));
        assert_eq!(d.build_id(), Some("9f2c1ab"));
        assert!(d.declared());
        let said = d.describe();
        assert_eq!(said["name"], "prod-eu");
        assert_eq!(said["build"], "9f2c1ab");
    }

    // `std::env::var("APP_BUILD").unwrap_or_default()` in a deployment that
    // never set the variable is the likely way to end up here, and a build
    // called "" would be a worse answer than no build at all
    #[test]
    fn an_empty_declaration_is_an_absence_and_not_a_name_of_nothing() {
        let d = Deployment::new().build("").name("  ");
        assert_eq!(d.build_id(), None);
        assert_eq!(d.called(), None);
        assert!(!d.declared());
    }

    #[test]
    fn the_platform_is_the_pair_this_build_was_compiled_for() {
        assert_eq!(
            Deployment::platform(),
            format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
        );
    }

    // the point of the split, asserted: hestan's version is not the
    // application's, and a caller reading one for the other is the whole
    // confusion this module exists to prevent
    #[test]
    fn hestans_own_version_is_not_the_declared_build() {
        let d = Deployment::new().name("prod-eu");
        assert_eq!(d.build_id(), None);
        assert!(!Deployment::hestan_version().is_empty());
    }
}

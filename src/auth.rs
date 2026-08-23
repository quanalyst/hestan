//! who may drive this deployment, and how they say who they are.
//!
//! the api launches runs, cancels them, pauses schedules and moves queue
//! positions. on loopback that is a process talking to itself and needs
//! nothing; on any other address it is a button on the internet that runs
//! arbitrary jobs. so the default is not "no authentication": it is **no
//! authenticator configured**, and [`serve`](crate::Hestan::serve) refuses any
//! address that is not loopback under it:
//!
//! ```no_run
//! # use hestan::{Auth, Hestan};
//! # async fn f(app: Hestan) -> Result<(), hestan::Error> {
//! // serves: one machine talking to itself, exactly as it always has
//! app.serve(([127, 0, 0, 1], 4000)).await
//! # }
//! # async fn g(app: Hestan) -> Result<(), hestan::Error> {
//! // refuses, naming the address and what to do about it
//! app.serve(([0, 0, 0, 0], 4000)).await
//! # }
//! # async fn h(app: Hestan) -> Result<(), hestan::Error> {
//! // serves: something checks who is asking
//! app.auth(Auth::bearer(std::env::var("HESTAN_TOKEN").unwrap()))
//!     .serve(([0, 0, 0, 0], 4000))
//!     .await
//! # }
//! ```
//!
//! a refusal rather than a warning because a warning is a line in a log that
//! scrolled past three deploys ago, and what it would have been warning about
//! is a stranger's run on your warehouse.
//!
//! # The two authenticators
//!
//! [`Auth::bearer`] is one token in an `Authorization: Bearer` header, for the
//! deployment with no identities of its own to lend. [`Auth::custom`] is a
//! closure over the request, for the host that already knows who its people
//! are: a header its proxy set, a signature it can check, a table it owns.
//! one is something to stand up in an afternoon; the other composes hestan
//! into what you already have rather than running a second scheme beside it.
//!
//! # The roles
//!
//! | role | may |
//! | --- | --- |
//! | [`Access::Viewer`] | read: every `GET` |
//! | [`Access::Operator`] | that, plus launch, cancel, retry, resume, build, backfill |
//! | [`Access::Admin`] | that, plus pause, unpause, priority, presets (what changes how the deployment behaves rather than what it is doing now) |
//!
//! `docs/auth.md` writes that out endpoint by endpoint. the code that enforces
//! it is derived from the table rather than the table from the code, and
//! anything the table does not name is a mutation and needs an operator: a
//! route added tomorrow lands on the rule and not in a hole.
//!
//! # The scopes
//!
//! the ladder says how much somebody may do and nothing about *what to*, and
//! there is one case it is too coarse for: a ci pipeline that may launch one
//! deploy job and must not be able to cancel a production run. a [`Scope`]
//! narrows the second question without touching the first, so an operator
//! scoped to `deploy` is an operator on `deploy` and a stranger everywhere
//! else:
//!
//! ```no_run
//! # use hestan::{Auth, Hestan, Identity, Scope};
//! # fn f(app: Hestan) -> Hestan {
//! app.auth(Auth::custom(|req| {
//!     match req.bearer()? {
//!         t if hestan::auth::secret_eq(t, "the-ci-token") => {
//!             Some(Identity::operator("ci").scoped_to(Scope::jobs(["deploy"])))
//!         }
//!         t if hestan::auth::secret_eq(t, "the-ops-token") => Some(Identity::admin("ops")),
//!         _ => None,
//!     }
//! }))
//! # }
//! ```
//!
//! **a scope limits what a token may change, and does nothing to what it may
//! read.** that is a decision rather than an omission, and the reasoning is in
//! `docs/auth.md`: a write names in its path what it is about, so one check in
//! one place can rule on every write there will ever be, and a read is mostly
//! a list, which can only be narrowed inside each handler that builds one. a
//! filter that has to be remembered at every list is a filter that will be
//! missed at one, and a confidentiality promise that holds in nine places out
//! of ten is worse than none, because people plan around it. so hestan makes
//! the small exact promise instead: **a scope is not a confidentiality
//! boundary**, and a token that can read reads the whole deployment.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::error::Error;

/// what someone may do, in the order the roles contain each other: an operator
/// may everything a viewer may, and how much of the api a request needs is
/// decided by `identity.role >= needed` and nothing else.
///
/// *what* it may be done to is the other question, and a [`Scope`] is where
/// that one is answered. the two are separate on purpose: a scope narrows a
/// role rather than replacing it, so nothing here changes meaning when one is
/// in play.
///
/// **a closed set**, and it stays one: the roles contain each other and the
/// ladder decision is this comparison, so a fourth role changes what these
/// three mean rather than adding to them, and a build error is the right way
/// to make a custom authenticator's mapping be read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    /// read anything, change nothing.
    Viewer,
    /// that, plus the things that drive work: launch, cancel, retry, resume,
    /// build, backfill.
    Operator,
    /// that, plus the things that change how the deployment behaves (pausing
    /// a schedule, moving a limit, editing a preset) as opposed to what it is
    /// doing right now.
    Admin,
}

impl Access {
    /// `viewer`, `operator` or `admin`: what the api sends and what a custom
    /// authenticator's own config is likely to be written in.
    pub fn as_str(&self) -> &'static str {
        match self {
            Access::Viewer => "viewer",
            Access::Operator => "operator",
            Access::Admin => "admin",
        }
    }
}

impl std::fmt::Display for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// which jobs and assets a token may change, for the token that should be able
/// to do one thing.
///
/// [`Scope::everything()`] is the default and is every identity that existed
/// before scopes did: unlimited, and exactly as it was. anything else is a
/// list, and **the list is the whole of what may be touched**: a scope naming
/// jobs may touch no asset, and a scope naming an asset may launch no job. a
/// name nothing matches is a scope that may change nothing at all, which is
/// the right way round for a thing whose job is to say no.
///
/// ```
/// # use hestan::Scope;
/// // an operator on `deploy`, a stranger everywhere else
/// let ci = Scope::jobs(["deploy"]);
/// assert!(ci.may_touch_job("deploy"));
/// assert!(!ci.may_touch_job("billing"));
/// assert!(!ci.may_touch_asset("deploy"));
///
/// // and both halves, for the token that owns one pipeline end to end
/// let owner = Scope::jobs(["etl"]).and_assets(["orders"]);
/// assert!(owner.may_touch_asset("orders"));
/// ```
///
/// **reads are not scoped.** see the [module docs](crate::auth) for why, and
/// `docs/auth.md` for the whole of it. a scope says what a token may change.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct Scope {
    /// nothing is narrowed: the jobs and assets lists are not consulted.
    everything: bool,
    jobs: BTreeSet<String>,
    assets: BTreeSet<String>,
}

impl Scope {
    /// no limit: what every identity is unless something says otherwise, and
    /// what an unscoped deployment keeps being.
    pub fn everything() -> Scope {
        Scope {
            everything: true,
            ..Scope::default()
        }
    }

    /// these jobs and no assets.
    pub fn jobs<S: Into<String>>(names: impl IntoIterator<Item = S>) -> Scope {
        Scope {
            everything: false,
            jobs: names.into_iter().map(Into::into).collect(),
            assets: BTreeSet::new(),
        }
    }

    /// these assets and no jobs.
    pub fn assets<S: Into<String>>(names: impl IntoIterator<Item = S>) -> Scope {
        Scope {
            everything: false,
            jobs: BTreeSet::new(),
            assets: names.into_iter().map(Into::into).collect(),
        }
    }

    /// these jobs as well.
    pub fn and_jobs<S: Into<String>>(mut self, names: impl IntoIterator<Item = S>) -> Scope {
        self.everything = false;
        self.jobs.extend(names.into_iter().map(Into::into));
        self
    }

    /// these assets as well.
    pub fn and_assets<S: Into<String>>(mut self, names: impl IntoIterator<Item = S>) -> Scope {
        self.everything = false;
        self.assets.extend(names.into_iter().map(Into::into));
        self
    }

    /// whether this narrows anything. `true` is the unscoped identity, whose
    /// requests are decided by the [role ladder](Access) alone, exactly as
    /// they were before scopes existed.
    pub fn is_everything(&self) -> bool {
        self.everything
    }

    /// whether a request that changes job `name` is in this scope.
    pub fn may_touch_job(&self, name: &str) -> bool {
        self.everything || self.jobs.contains(name)
    }

    /// whether a request that changes asset `name` is in this scope.
    pub fn may_touch_asset(&self, name: &str) -> bool {
        self.everything || self.assets.contains(name)
    }
}

impl std::fmt::Display for Scope {
    /// what a refusal says the token was limited to: `job deploy`,
    /// `assets a and b`, `jobs etl, load and asset orders`, or `nothing`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.everything {
            return f.write_str("everything");
        }
        let mut parts: Vec<String> = Vec::new();
        for (what, names) in [("job", &self.jobs), ("asset", &self.assets)] {
            if names.is_empty() {
                continue;
            }
            let plural = if names.len() == 1 { "" } else { "s" };
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            parts.push(format!("{what}{plural} {}", names.join(", ")));
        }
        match parts.is_empty() {
            true => f.write_str("nothing"),
            false => f.write_str(&parts.join(" and ")),
        }
    }
}

/// who is asking: a name for the audit trail, a [role](Access) for how much
/// they may do, and a [scope](Scope) for what to.
///
/// the name is what the event log records and what the ui shows. it is never a
/// credential (see [`Auth::bearer`]) and it is never invented: a deployment
/// with no authenticator records no actor at all rather than a name that means
/// nothing.
///
/// **hestan builds this, never a request.** an [authenticator](Auth) is the
/// only thing that produces one, it runs in the deployment's own process, and
/// nothing in a header or a body reaches the scope on it. that is what makes a
/// scope something the holder of a token cannot widen: there is no input for
/// them to widen it through.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Identity {
    /// what goes in the audit trail. use something a person would recognize
    /// six months later in an event log, never the credential itself.
    pub name: String,
    /// what they may do.
    pub role: Access,
    /// what they may do it to. [`Scope::everything()`] unless
    /// [`scoped_to`](Identity::scoped_to) said otherwise, which is every
    /// identity a deployment built before this field existed.
    pub scope: Scope,
}

impl Identity {
    /// somebody, with a role, over the whole deployment.
    pub fn new(name: impl Into<String>, role: Access) -> Identity {
        Identity {
            name: name.into(),
            role,
            scope: Scope::everything(),
        }
    }

    /// the same identity, limited to the jobs and assets `scope` names.
    ///
    /// ```
    /// # use hestan::{Identity, Scope};
    /// Identity::operator("ci").scoped_to(Scope::jobs(["deploy"]));
    /// ```
    pub fn scoped_to(mut self, scope: Scope) -> Identity {
        self.scope = scope;
        self
    }

    /// somebody who may only read.
    pub fn viewer(name: impl Into<String>) -> Identity {
        Identity::new(name, Access::Viewer)
    }

    /// somebody who may drive work but not change the deployment.
    pub fn operator(name: impl Into<String>) -> Identity {
        Identity::new(name, Access::Operator)
    }

    /// somebody who may do anything the api offers.
    pub fn admin(name: impl Into<String>) -> Identity {
        Identity::new(name, Access::Admin)
    }
}

/// what a [custom authenticator](Auth::custom) is shown: the request, minus
/// its body.
///
/// no body because a credential is not in one, and because reading it here
/// would consume it before the handler that needs it. everything an
/// authenticator has to look at is a header, a path or a method.
pub struct Request<'a> {
    method: &'a str,
    path: &'a str,
    headers: &'a axum::http::HeaderMap,
}

impl<'a> Request<'a> {
    pub(crate) fn new(
        method: &'a str,
        path: &'a str,
        headers: &'a axum::http::HeaderMap,
    ) -> Request<'a> {
        Request {
            method,
            path,
            headers,
        }
    }

    /// the method in capitals: `GET`, `POST`, `PUT`, `DELETE`.
    pub fn method(&self) -> &str {
        self.method
    }

    /// the path, with no query string: `/api/runs/019.../cancel`.
    pub fn path(&self) -> &str {
        self.path
    }

    /// one header by name, case-insensitively, or `None` when it is absent or
    /// is not valid utf-8.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// the token from `Authorization: Bearer <token>`, for an authenticator
    /// that looks one up rather than being handed the identity.
    ///
    /// compare it with [`secret_eq`], not with `==`.
    pub fn bearer(&self) -> Option<&str> {
        let value = self.header("authorization")?;
        let (scheme, token) = value.split_once(' ')?;
        scheme
            .eq_ignore_ascii_case("bearer")
            .then_some(token.trim())
    }
}

/// a host's own check, from [`Auth::custom`].
#[derive(Clone)]
pub struct Check(Checker);

type Checker = Arc<dyn Fn(&Request<'_>) -> Option<Identity> + Send + Sync>;

impl std::fmt::Debug for Check {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Check(…)")
    }
}

/// one token, kept as its digest.
///
/// the plaintext is hashed by [`Auth::bearer`] and dropped there, so from that
/// line on the process does not hold the secret it is checking against: a core
/// dump, a `Debug` somebody adds in a hurry or a read of this process's memory
/// finds a sha-256 digest and not the token.
#[derive(Clone)]
pub struct Token([u8; 32]);

impl Token {
    /// whether `presented` is this token, compared in constant time.
    pub fn matches(&self, presented: &str) -> bool {
        ct_eq(&digest(presented), &self.0)
    }
}

// nothing in this file derives `Debug`, and this is why: the way a credential
// reaches a log is something printing the struct that holds it. a digest is not
// the token, but it is guessable offline and there is no reason for it to be
// printable either
impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(…)")
    }
}

/// what checks who is asking.
///
/// ```no_run
/// # use hestan::{Auth, Hestan};
/// # async fn f(app: Hestan) -> Result<(), hestan::Error> {
/// app.auth(Auth::bearer(std::env::var("HESTAN_TOKEN").expect("a token")))
///     .serve(([0, 0, 0, 0], 4000))
///     .await
/// # }
/// ```
///
/// there is no default variant: a deployment either configures one of these or
/// serves loopback only. see the [module docs](crate::auth).
///
/// **not a closed set** (`#[non_exhaustive]`). another way of checking who
/// is asking is a variant here, and nothing is lost by that: this is
/// configured rather than read back, and the two variants carrying anything
/// carry values only this crate builds.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Auth {
    /// nothing in hestan checks identity, deliberately.
    ///
    /// this is an assertion about what is in front of hestan (a proxy that
    /// authenticates, a mesh doing mtls, a network nobody else is on), and it
    /// turns the refusal off for every address. it is spelled out rather than
    /// implied because the difference between "I have thought about this" and
    /// "I did not know" is the whole of what the refusal is for.
    None,
    /// one token, from [`Auth::bearer`].
    Bearer(Token),
    /// a host's own check, from [`Auth::custom`].
    Custom(Check),
}

impl Auth {
    /// one shared token, presented as `Authorization: Bearer <token>`, and it
    /// is an [admin](Access::Admin) token.
    ///
    /// ```no_run
    /// # use hestan::{Auth, Hestan};
    /// # fn f(app: Hestan) -> Hestan {
    /// app.auth(Auth::bearer(std::env::var("HESTAN_TOKEN").expect("a token")))
    /// # }
    /// ```
    ///
    /// take it from the environment or a secret file rather than writing a
    /// literal: a token in argv is a token in `ps`, and a token in source is
    /// a token in git. hestan hashes it here and never holds the plaintext,
    /// never logs it, never puts it in an event, an error or a response body,
    /// and never sends it to the ui: the only copies anywhere are the one you
    /// configured and the one whoever is asking presents.
    ///
    /// one token is one identity, named `bearer`, and everyone holding it is
    /// that identity, which is why the audit trail says "somebody with the
    /// token" rather than a person's name. [`custom`](Auth::custom) is where
    /// names and read-only roles come from; hestan has no user store and is
    /// not going to grow one.
    pub fn bearer(token: impl AsRef<str>) -> Auth {
        Auth::Bearer(Token(digest(token.as_ref())))
    }

    /// hestan's decision, from the host's own check.
    ///
    /// the closure sees each request's method, path and headers and answers
    /// with an [`Identity`] or `None`, which is a 401. this is how a
    /// deployment that already authenticates composes hestan into what it has:
    ///
    /// ```no_run
    /// # use hestan::{Access, Auth, Hestan, Identity};
    /// # fn f(app: Hestan) -> Hestan {
    /// app.auth(Auth::custom(|req| {
    ///     // whatever the thing in front of this promises it has checked
    ///     let user = req.header("x-forwarded-user")?;
    ///     let role = match req.header("x-forwarded-groups").unwrap_or_default() {
    ///         groups if groups.contains("ops") => Access::Admin,
    ///         _ => Access::Viewer,
    ///     };
    ///     Some(Identity::new(user, role))
    /// }))
    /// # }
    /// ```
    ///
    /// it runs on the request path, so it must not block: a lookup that costs
    /// a network round trip belongs in the thing in front of hestan, where its
    /// answer is already being taken. and if it compares a secret of its own,
    /// compare it with [`secret_eq`].
    pub fn custom(f: impl Fn(&Request<'_>) -> Option<Identity> + Send + Sync + 'static) -> Auth {
        Auth::Custom(Check(Arc::new(f)))
    }

    /// who this request is, or `None` for nobody this deployment knows.
    ///
    /// [`Auth::None`] is nobody too: an assertion that something else checked
    /// is not an identity, and inventing one here is what would put a name
    /// that means nothing on every event in the log.
    pub(crate) fn identify(&self, req: &Request<'_>) -> Option<Identity> {
        match self {
            Auth::None => None,
            Auth::Bearer(token) => {
                let presented = req.bearer()?;
                token.matches(presented).then(|| Identity::admin("bearer"))
            }
            Auth::Custom(check) => (check.0)(req),
        }
    }

    /// whether this checks anything at all. [`Auth::None`] does not, and every
    /// request under it is served with no identity, the same as a deployment
    /// that configured nothing and is therefore on loopback.
    pub(crate) fn checks(&self) -> bool {
        !matches!(self, Auth::None)
    }

    /// what a 401 from this one offers, for the `WWW-Authenticate` header.
    ///
    /// `None` for a custom authenticator: it knows what it reads and hestan
    /// does not, and naming a scheme it is not using would be a lie in a
    /// header clients act on.
    pub(crate) fn challenge(&self) -> Option<&'static str> {
        matches!(self, Auth::Bearer(_)).then_some("Bearer")
    }
}

/// compare two secrets in constant time.
///
/// **not `==`.** a byte-by-byte comparison stops at the first byte that
/// differs, so how long it took to say no is how much of the token was right,
/// and enough requests turn that into the token itself. this hashes both sides
/// and compares the digests, which takes the same time for every input and
/// leaks neither the content nor the length.
pub fn secret_eq(a: &str, b: &str) -> bool {
    ct_eq(&digest(a), &digest(b))
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

/// every byte of both, whatever they are: the accumulator goes through
/// `black_box` so nothing can turn the fold back into an early return.
fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    std::hint::black_box(diff) == 0
}

/// whether an address can only be reached from this machine.
///
/// the forms that are the same socket and do not look alike: `127.0.0.1` and
/// the rest of `127.0.0.0/8`, `::1`, and `::ffff:127.0.0.1`, v4 loopback
/// wearing a v6 address, which `Ipv6Addr::is_loopback` says nothing about and
/// which a listener does receive v4 loopback traffic on.
///
/// `0.0.0.0` and `[::]` are every interface this machine has, which is the
/// case the refusal exists for.
fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

/// what serving `addr` under this configuration does: nothing to say, one line
/// to say first, or a refusal.
///
/// **`addr` is the address a listener is holding**, not the one somebody asked
/// for. the two are the same today, and this is the check the deployment's
/// safety rests on: it belongs on the address requests will actually arrive
/// on, so that nothing put between the ask and the bind can ever make the
/// guarded address and the served one two different things.
pub(crate) fn guard(addr: SocketAddr, auth: Option<&Auth>) -> Result<Option<String>, Error> {
    match auth {
        None if is_loopback(addr.ip()) => Ok(None),
        None => Err(Error::Unguarded(addr)),
        // the opt-out, said back: it is a claim about something else, and a
        // claim nobody hears again is a claim nobody re-examines
        Some(Auth::None) if !is_loopback(addr.ip()) => Ok(Some(format!(
            "serving {addr} with Auth::None: nothing in hestan checks who is asking, and \
             whatever is in front of it is what stops a stranger launching runs here"
        ))),
        Some(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn identify(auth: &Auth, headers: &HeaderMap) -> Option<Identity> {
        auth.identify(&Request::new("GET", "/api/runs", headers))
    }

    fn bearer_header(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        headers
    }

    #[test]
    fn every_spelling_of_this_machine_is_loopback_and_every_interface_is_not() {
        for one in [
            "127.0.0.1:4000",
            // the whole of 127/8, not only the address people type
            "127.9.9.9:4000",
            "[::1]:4000",
            // v4 loopback wearing a v6 address
            "[::ffff:127.0.0.1]:4000",
        ] {
            assert!(is_loopback(addr(one).ip()), "{one} is this machine");
        }
        for anyone in [
            "0.0.0.0:4000",
            "[::]:4000",
            "192.168.1.10:4000",
            "10.0.0.4:4000",
            "[2001:db8::1]:4000",
            // and the mapped forms of those, which are not loopback for the
            // same reason their v4 spellings are not
            "[::ffff:0.0.0.0]:4000",
            "[::ffff:192.168.1.10]:4000",
        ] {
            assert!(
                !is_loopback(addr(anyone).ip()),
                "{anyone} is not this machine"
            );
        }
    }

    #[test]
    fn an_address_anyone_can_reach_is_refused_until_something_guards_it() {
        // loopback serves with nothing configured, exactly as it always has
        for one in ["127.0.0.1:4000", "[::1]:4000", "[::ffff:127.0.0.1]:4000"] {
            assert!(matches!(guard(addr(one), None), Ok(None)), "{one}");
        }

        for anyone in ["0.0.0.0:4000", "[::]:4000", "192.168.1.10:4000"] {
            let said = guard(addr(anyone), None).unwrap_err().to_string();
            // it names the address, or the reader has to guess which of the
            // two in their config it means, and what to do instead
            assert!(said.contains(anyone), "{said}");
            assert!(said.contains("Hestan::auth"), "{said}");
        }

        // and an authenticator turns it into an ordinary serve
        let guarded = Auth::bearer("s3cret");
        assert!(matches!(
            guard(addr("0.0.0.0:4000"), Some(&guarded)),
            Ok(None)
        ));
    }

    #[test]
    fn the_opt_out_serves_and_says_what_it_is_leaning_on() {
        let said = guard(addr("0.0.0.0:4000"), Some(&Auth::None))
            .unwrap()
            .expect("an opt-out on a reachable address says so");
        assert!(said.contains("0.0.0.0:4000"), "{said}");
        assert!(said.contains("Auth::None"), "{said}");
        // on loopback it is the ordinary case and there is nothing to say
        assert!(matches!(
            guard(addr("127.0.0.1:4000"), Some(&Auth::None)),
            Ok(None)
        ));
    }

    #[test]
    fn a_token_matches_itself_and_nothing_else() {
        let Auth::Bearer(token) = Auth::bearer("s3cret") else {
            panic!("bearer is a bearer");
        };
        assert!(token.matches("s3cret"));
        // a wrong token, a prefix of the right one, and one byte longer
        assert!(!token.matches("s3crft"));
        assert!(!token.matches("s3cre"));
        assert!(!token.matches("s3cret "));
        assert!(!token.matches(""));
    }

    #[test]
    fn a_bearer_token_is_the_only_thing_that_identifies_as_one() {
        let auth = Auth::bearer("s3cret");
        assert_eq!(
            identify(&auth, &bearer_header("s3cret")),
            Some(Identity::admin("bearer"))
        );
        // a wrong token, a prefix of the right one, and no header at all are
        // one answer: nobody
        assert_eq!(identify(&auth, &bearer_header("s3crft")), None);
        assert_eq!(identify(&auth, &bearer_header("s3cre")), None);
        assert_eq!(identify(&auth, &bearer_header("")), None);
        assert_eq!(identify(&auth, &HeaderMap::new()), None);

        // the scheme word is case-insensitive; the token is not
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer s3cret".parse().unwrap());
        assert!(identify(&auth, &headers).is_some());
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer S3CRET".parse().unwrap());
        assert!(identify(&auth, &headers).is_none());
    }

    #[test]
    fn a_custom_authenticator_names_who_it_recognized() {
        let auth = Auth::custom(|req| match req.header("x-user")? {
            "ada" => Some(Identity::admin("ada")),
            "bob" => Some(Identity::viewer("bob")),
            _ => None,
        });
        let named = |who: &str| {
            let mut headers = HeaderMap::new();
            headers.insert("x-user", who.parse().unwrap());
            identify(&auth, &headers)
        };
        assert_eq!(named("ada").unwrap().role, Access::Admin);
        assert_eq!(named("bob").unwrap().name, "bob");
        assert_eq!(named("nobody"), None);
        assert_eq!(identify(&auth, &HeaderMap::new()), None);
        // and it is asked nothing about a scheme it is not using
        assert_eq!(auth.challenge(), None);
        assert_eq!(Auth::bearer("s3cret").challenge(), Some("Bearer"));
    }

    // the opt-out identifies nobody rather than inventing somebody, which is
    // what keeps a made-up name off every event in an unauthenticated log
    #[test]
    fn the_opt_out_identifies_nobody_and_checks_nothing() {
        assert_eq!(identify(&Auth::None, &bearer_header("s3cret")), None);
        assert!(!Auth::None.checks());
        assert!(Auth::bearer("s3cret").checks());
    }

    // the roles contain each other, and every decision in the server is this
    // comparison
    #[test]
    fn a_role_may_whatever_the_ones_below_it_may() {
        assert!(Access::Admin > Access::Operator);
        assert!(Access::Operator > Access::Viewer);
        assert!(Access::Viewer >= Access::Viewer);
    }

    #[test]
    fn secrets_compare_equal_only_to_themselves() {
        assert!(secret_eq("s3cret", "s3cret"));
        assert!(!secret_eq("s3cret", "s3crft"));
        // a length difference is not an early answer here: both sides are
        // hashed to the same 32 bytes before anything is compared
        assert!(!secret_eq("s3cret", "s3cret "));
        assert!(!secret_eq("", "s3cret"));
        assert!(secret_eq("", ""));
    }
}

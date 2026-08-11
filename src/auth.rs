//! who may drive this deployment, and how they say who they are.
//!
//! the api launches runs, cancels them, pauses schedules and moves queue
//! positions. on loopback that is a process talking to itself and needs
//! nothing; on any other address it is a button on the internet that runs
//! arbitrary jobs. so the default is not "no authentication" — it is **no
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

use std::net::{IpAddr, SocketAddr};

use sha2::{Digest, Sha256};

use crate::error::Error;

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
/// there is no default variant: a deployment either configures one of these or
/// serves loopback only. see the [module docs](crate::auth).
#[derive(Debug, Clone)]
pub enum Auth {
    /// nothing in hestan checks identity, deliberately.
    ///
    /// this is an assertion about what is in front of hestan — a proxy that
    /// authenticates, a mesh doing mtls, a network nobody else is on — and it
    /// turns the refusal off for every address. it is spelled out rather than
    /// implied because the difference between "I have thought about this" and
    /// "I did not know" is the whole of what the refusal is for.
    None,
    /// one token, from [`Auth::bearer`].
    Bearer(Token),
}

impl Auth {
    /// one shared token, presented as `Authorization: Bearer <token>`.
    ///
    /// ```no_run
    /// # use hestan::{Auth, Hestan};
    /// # fn f(app: Hestan) -> Hestan {
    /// app.auth(Auth::bearer(std::env::var("HESTAN_TOKEN").expect("a token")))
    /// # }
    /// ```
    ///
    /// take it from the environment or a secret file rather than writing a
    /// literal — a token in argv is a token in `ps`, and a token in source is
    /// a token in git. hestan hashes it here and never holds the plaintext,
    /// never logs it, never puts it in an event, an error or a response body,
    /// and never sends it to the ui: the only copies anywhere are the one you
    /// configured and the one whoever is asking presents.
    pub fn bearer(token: impl AsRef<str>) -> Auth {
        Auth::Bearer(Token(digest(token.as_ref())))
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
/// the rest of `127.0.0.0/8`, `::1`, and `::ffff:127.0.0.1` — v4 loopback
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

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
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

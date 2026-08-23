//! whose work this is: the namespace that divides a deployment, and the owner
//! a failure hook is handed.
//!
//! a namespace is an `Option<String>` on a job, an asset, a schedule and a
//! sensor, and there is one function here that decides whether a declared one
//! is allowed. an [`Owner`] is a small struct, re-exported at the crate root
//! like every other type hestan hands back. the concepts are written up on the
//! public items that carry them and in `docs/namespaces.md`.

use serde::{Deserialize, Serialize};

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

/// who to wake when something a job or an asset does goes wrong.
///
/// a team, a person, or both, plus how to reach them. that is the whole shape,
/// and it is deliberately this small: **hestan carries this and hands it to a
/// hook. it is not a directory service and it does not resolve anything.** the
/// strings mean whatever the thing on the other end of your hook makes of
/// them, and hestan never parses, validates or dials one.
///
/// ```
/// # use hestan::Owner;
/// let owner = Owner::team("data-platform")
///     .person("ada")
///     .contact("#data-alerts")
///     .escalates_to("ops@example.com");
/// assert_eq!(owner.to_string(), "ada of data-platform (#data-alerts)");
/// ```
///
/// declare it with [`JobBuilder::owner`](crate::JobBuilder::owner) or
/// [`Asset::owner`](crate::Asset::owner). a run's terminal event carries the
/// owner of the job it was a run of ([`RunEvent::owner`](crate::RunEvent)),
/// and a freshness alert carries the owner of whatever went late
/// ([`LateEvent::owner`](crate::LateEvent)), so a hook reads it off the event
/// rather than being handed it by whoever registered the hook.
///
/// # Escalation, and where the line is
///
/// [`escalates_to`](Owner::escalates_to) is **a second contact string, and
/// nothing else happens to it**. hestan carries it, puts it on the event, shows
/// it on the page and hands it to your hook. it does not wait, does not ask
/// whether the first contact answered, does not acknowledge, does not repeat,
/// and has no notion of a rotation or a shift.
///
/// that is the line, drawn on purpose. timers, acknowledgements and rotations
/// are a paging product, they are somebody's whole company, and a half of one
/// inside an orchestrator would be the worst of both: something that looks
/// like it will keep trying and does not. what hestan promises is that the
/// second contact reaches the hook along with the first, and what to do with
/// it is the hook's decision.
///
/// **fields are private and there are accessors**, unlike the row types
/// hestan hands back: this is a struct callers build, and a struct callers
/// build must be able to gain a field without breaking every literal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    #[serde(skip_serializing_if = "Option::is_none")]
    team: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    person: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    escalates_to: Option<String>,
}

impl Owner {
    /// a team owns this.
    pub fn team(name: impl Into<String>) -> Owner {
        Owner {
            team: Some(name.into()),
            ..Owner::default()
        }
    }

    /// a person owns this.
    ///
    /// used as a constructor or, on an owner that already names a team, as
    /// the person inside it: `Owner::team("data").person("ada")`.
    pub fn person(mut self, name: impl Into<String>) -> Owner {
        self.person = Some(name.into());
        self
    }

    /// how to reach them: a channel, an address, a url, a rota's name.
    /// whatever the thing on the other end of your hook understands.
    pub fn contact(mut self, how: impl Into<String>) -> Owner {
        self.contact = Some(how.into());
        self
    }

    /// who to try when the first contact does not answer.
    ///
    /// **metadata, and only metadata.** nothing in hestan waits, retries or
    /// notices that nobody answered: this string reaches the hook beside the
    /// first contact and the hook decides. see the type's docs for why that is
    /// where the line is.
    pub fn escalates_to(mut self, how: impl Into<String>) -> Owner {
        self.escalates_to = Some(how.into());
        self
    }

    /// the team, if one was named.
    pub fn team_name(&self) -> Option<&str> {
        self.team.as_deref()
    }

    /// the person, if one was named.
    pub fn person_name(&self) -> Option<&str> {
        self.person.as_deref()
    }

    /// how to reach them, if that was said.
    pub fn contact_at(&self) -> Option<&str> {
        self.contact.as_deref()
    }

    /// the second contact, if there is one. hestan does nothing with it.
    pub fn escalation(&self) -> Option<&str> {
        self.escalates_to.as_deref()
    }

    /// who this is, in words, with no contact on the end: `ada of
    /// data-platform`, `data-platform`, `ada`, or `nobody named` for an owner
    /// that named neither.
    pub fn who(&self) -> String {
        match (self.person.as_deref(), self.team.as_deref()) {
            (Some(person), Some(team)) => format!("{person} of {team}"),
            (Some(person), None) => person.to_string(),
            (None, Some(team)) => team.to_string(),
            (None, None) => "nobody named".to_string(),
        }
    }
}

impl std::fmt::Display for Owner {
    /// [`who`](Owner::who), with the contact in brackets after it where there
    /// is one. this is the phrase an alert line ends in.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.who())?;
        match &self.contact {
            Some(contact) => write!(f, " ({contact})"),
            None => Ok(()),
        }
    }
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

    // the one line an alert ends in, in each of the shapes a declaration can
    // take. an absent half is absent rather than an empty pair of brackets
    #[test]
    fn an_owner_says_itself_in_one_line_whichever_half_was_declared() {
        let full = Owner::team("data-platform")
            .person("ada")
            .contact("#data-alerts");
        assert_eq!(full.who(), "ada of data-platform");
        assert_eq!(full.to_string(), "ada of data-platform (#data-alerts)");

        assert_eq!(Owner::team("data").to_string(), "data");
        assert_eq!(
            Owner::default().person("ada").to_string(),
            "ada",
            "a person alone should not be dressed up as a team"
        );
        assert_eq!(
            Owner::team("data").contact("ops@example.com").to_string(),
            "data (ops@example.com)"
        );
        // an owner that named nobody is a declaration somebody wrote by
        // accident, and it says so rather than rendering as a blank
        assert_eq!(Owner::default().to_string(), "nobody named");
    }

    // the escalation contact is carried, reachable and inert: nothing here
    // waits on it, and the type's docs are where that line is drawn
    #[test]
    fn an_escalation_contact_is_carried_and_nothing_else() {
        let owner = Owner::team("data")
            .contact("#data-alerts")
            .escalates_to("ops@example.com");
        assert_eq!(owner.contact_at(), Some("#data-alerts"));
        assert_eq!(owner.escalation(), Some("ops@example.com"));
        // and it is not in the one line, which is the first contact's: a page
        // that printed both would read as if hestan had already tried one
        assert_eq!(owner.to_string(), "data (#data-alerts)");
        assert_eq!(Owner::team("data").escalation(), None);
    }

    // it goes into a notification payload and comes back out of one, and an
    // absent half is an absent key rather than a null
    #[test]
    fn an_owner_round_trips_through_json_without_writing_down_what_was_not_said() {
        let owner = Owner::team("data").contact("#alerts");
        let json = serde_json::to_value(&owner).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"team": "data", "contact": "#alerts"})
        );
        assert_eq!(serde_json::from_value::<Owner>(json).unwrap(), owner);
        // and an empty object is an owner that named nothing, not an error:
        // a payload written by an older hestan has no owner key at all
        assert_eq!(
            serde_json::from_value::<Owner>(serde_json::json!({})).unwrap(),
            Owner::default()
        );
    }
}

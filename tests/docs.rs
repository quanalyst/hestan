//! the two lists that go stale silently.
//!
//! a page nobody links to is a page nobody reads and therefore nobody updates,
//! and a feature the readme does not mention is one nobody turns on. neither
//! shows up in a compile, a clippy pass or any other test here: the file is
//! still valid markdown and the crate still builds. so they are asserted.

use std::collections::BTreeSet;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    let path = root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// every page in `docs/`, by file name, index excluded.
fn pages() -> BTreeSet<String> {
    std::fs::read_dir(root().join("docs"))
        .expect("docs/")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_string_lossy().into_owned();
            (name.ends_with(".md") && name != "README.md").then_some(name)
        })
        .collect()
}

/// every `foo.md` the index links to.
fn indexed() -> BTreeSet<String> {
    read("docs/README.md")
        .split("](")
        .skip(1)
        .filter_map(|rest| {
            let link = rest.split(')').next()?;
            link.ends_with(".md").then(|| link.to_string())
        })
        .collect()
}

#[test]
fn the_index_lists_every_page_and_nothing_else() {
    let (pages, indexed) = (pages(), indexed());
    let unlisted: Vec<&String> = pages.difference(&indexed).collect();
    assert!(
        unlisted.is_empty(),
        "docs/README.md does not link to: {unlisted:?}"
    );
    let gone: Vec<&String> = indexed.difference(&pages).collect();
    assert!(gone.is_empty(), "docs/README.md links to nothing: {gone:?}");
    // and something was found, so a scraper that stopped finding links cannot
    // pass by comparing two empty sets
    assert!(pages.len() > 20, "only {} pages found", pages.len());
}

/// the feature names `Cargo.toml` declares, `default` excluded: it is a list
/// of the others rather than a thing of its own.
fn declared_features() -> BTreeSet<String> {
    read("Cargo.toml")
        .split("\n[")
        .find(|section| section.starts_with("features]"))
        .expect("[features] in Cargo.toml")
        .lines()
        .skip(1)
        .filter_map(|line| {
            let name = line.split('=').next()?.trim();
            (!name.is_empty() && !name.starts_with('#') && name != "default")
                .then(|| name.to_string())
        })
        .collect()
}

/// the features the readme's table names, read out of its first column.
///
/// found by its header rather than by shape, so another table of backticked
/// names elsewhere in the readme is not mistaken for this one.
fn documented_features() -> BTreeSet<String> {
    read("README.md")
        .split_once("| feature | what it adds |\n")
        .expect("the feature table in README.md")
        .1
        .lines()
        .skip(1)
        .take_while(|line| line.starts_with('|'))
        .filter_map(|line| Some(line.strip_prefix("| `")?.split('`').next()?.to_string()))
        .collect()
}

// a feature that exists and is not written down is one nobody turns on; a
// feature written down that no longer exists is a build error for whoever
// copies the line
#[test]
fn the_readme_lists_exactly_the_features_the_manifest_has() {
    let (declared, documented) = (declared_features(), documented_features());
    assert_eq!(
        declared, documented,
        "the readme's feature table and Cargo.toml disagree"
    );
    assert!(!declared.is_empty(), "no features found in Cargo.toml");
}

/// whether this build compiled `name`, decided here rather than read off the
/// library, so that the two lists have to agree rather than one of them being
/// both the claim and the check.
///
/// `None` is a feature this test has never heard of, which is what a feature
/// added to `Cargo.toml` and nowhere else looks like from in here.
fn compiled(name: &str) -> Option<bool> {
    Some(match name {
        "bundled" => cfg!(feature = "bundled"),
        "capture" => cfg!(feature = "capture"),
        "cli" => cfg!(feature = "cli"),
        "dbt" => cfg!(feature = "dbt"),
        "http" => cfg!(feature = "http"),
        "otel" => cfg!(feature = "otel"),
        "parquet" => cfg!(feature = "parquet"),
        "postgres" => cfg!(feature = "postgres"),
        _ => return None,
    })
}

// `GET /api/health` and `hestan doctor` report which features a deployment
// compiled, and a report that had drifted from the build would be worse than
// no report: it is read by somebody asking why a deployment does not have a
// thing it does have. so the manifest, the library and this file all have to
// say the same words.
#[test]
fn the_feature_set_reported_is_the_feature_set_compiled() {
    let declared = declared_features();
    assert!(!declared.is_empty(), "no features found in Cargo.toml");
    let reported: BTreeSet<String> = hestan::Deployment::features()
        .into_iter()
        .map(str::to_string)
        .collect();

    // nothing is reported that is not a feature this crate has
    let invented: Vec<&String> = reported.difference(&declared).collect();
    assert!(
        invented.is_empty(),
        "reported but not declared: {invented:?}"
    );

    // and every declared feature is reported exactly when it was compiled,
    // which is what makes this a check on the build rather than on a list
    for name in &declared {
        let on = compiled(name)
            .unwrap_or_else(|| panic!("{name} is in Cargo.toml and unknown to this test"));
        assert_eq!(
            reported.contains(name),
            on,
            "{name}: compiled {on}, reported {}",
            reported.contains(name)
        );
    }
}

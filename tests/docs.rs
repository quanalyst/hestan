//! the lists that go stale silently, and the file list nobody reads.
//!
//! a page nobody links to is a page nobody reads and therefore nobody updates,
//! and a feature the readme does not mention is one nobody turns on. neither
//! shows up in a compile, a clippy pass or any other test here: the file is
//! still valid markdown and the crate still builds. so they are asserted.
//!
//! the third one is what `cargo package` puts in the `.crate`, which is the
//! only artifact anybody outside this repository ever gets. it is wrong in two
//! directions and they fail differently: a file that should not ship is a
//! quiet cost on every download, and a file that should ship and does not is a
//! crate that compiles here and for nobody else.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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
///
/// a feature is a line `name = ...` in the `[features]` section, and the name
/// has to look like one. cargo rewrites the manifest when it packages the
/// crate, and the rewrite puts a two-element array on four lines, so the three
/// lines under `cli = [` are part of `cli` rather than three features called
/// `"dep:clap",`, `"dep:reqwest",` and `]`. the repository's own manifest
/// keeps each on one line and the difference does not show there, which is
/// exactly why this is read strictly: the published crate ships the rewritten
/// one and runs this same test.
fn declared_features() -> BTreeSet<String> {
    read("Cargo.toml")
        .split("\n[")
        .find(|section| section.starts_with("features]"))
        .expect("[features] in Cargo.toml")
        .lines()
        .skip(1)
        .filter_map(|line| {
            let name = line.split_once('=')?.0.trim();
            let identifier = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
            (identifier && name != "default").then(|| name.to_string())
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

/// the top-level names a `.crate` may hold, and what each is there for.
///
/// a directory here ships whole. the rule the manifest's `include` list
/// implements is that compiling, testing and reading the library is what
/// ships, and building or checking *the repository* is not: the container
/// image, the compose stack, the check scripts, the kubernetes manifests, the
/// ci workflow and the ui's typescript sources are all one clone away at the
/// `repository` url and none of them is opened by a `cargo build`.
const ROOTS: [&str; 5] = ["docs", "examples", "src", "tests", "ui/dist"];

/// and the loose files, which are the manifest, its lockfile, the two cargo
/// writes itself, and the four a reader is owed.
const FILES: [&str; 8] = [
    ".cargo_vcs_info.json",
    "CHANGELOG.md",
    "Cargo.lock",
    "Cargo.toml",
    "Cargo.toml.orig",
    "LICENSE",
    "README.md",
    "SECURITY.md",
];

/// every path `cargo package` would put in the `.crate`.
///
/// asked of cargo rather than worked out from the `include` list here, because
/// a reimplementation of cargo's glob matching would agree with itself and
/// prove nothing. `--list` neither builds nor uploads; the target directory is
/// a throwaway so this cannot contend with the build that is running it.
fn packaged() -> BTreeSet<String> {
    let scratch = tempfile::tempdir().expect("a scratch target dir");
    let out = std::process::Command::new(env!("CARGO"))
        .args(["package", "--list", "--allow-dirty", "--offline"])
        .current_dir(root())
        .env("CARGO_TARGET_DIR", scratch.path())
        .output()
        .expect("cargo package --list");
    assert!(
        out.status.success(),
        "cargo package --list failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .map(str::to_string)
        .collect()
}

/// every file under `rel`, as a path relative to the repository root.
fn files_under(rel: &str) -> BTreeSet<String> {
    fn walk(dir: &Path, prefix: &str, into: &mut BTreeSet<String>) {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("a dir entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = format!("{prefix}/{name}");
            if entry.file_type().expect("a file type").is_dir() {
                walk(&entry.path(), &path, into);
            } else {
                into.insert(path);
            }
        }
    }
    let mut found = BTreeSet::new();
    walk(&root().join(rel), rel, &mut found);
    found
}

// a `.crate` is downloaded by everybody who depends on hestan and read by
// nobody, so a directory that drifts into it stays there. this fails on the
// next one rather than on somebody noticing. what it proves is the list: that
// each of these roots belongs in a published crate is the judgement written
// above them, and no test can make that for anybody.
#[test]
fn nothing_ships_that_is_not_a_root_or_a_file_named_here() {
    let packaged = packaged();
    assert!(packaged.len() > 50, "only {} files listed", packaged.len());
    let stray: Vec<&String> = packaged
        .iter()
        .filter(|path| {
            !FILES.contains(&path.as_str())
                && !ROOTS
                    .iter()
                    .any(|root| path.starts_with(&format!("{root}/")))
        })
        .collect();
    assert!(
        stray.is_empty(),
        "these ship and are in neither ROOTS nor FILES; add them there on purpose or \
         exclude them in Cargo.toml's `include`: {stray:?}"
    );
}

// the other direction, and the one that is not recoverable by a reader: a
// crate missing a file it compiles against does not build for anybody, and
// the file list looks fine because what is missing is not on it. this is the
// coarse half of that, over whole roots; the one below it is the exact half,
// and neither is a substitute for building the `.crate` (`just release-check`).
#[test]
fn nothing_under_a_root_named_here_is_left_out_of_the_crate() {
    let packaged = packaged();
    for root in ROOTS {
        let missing: Vec<String> = files_under(root)
            .into_iter()
            .filter(|path| !packaged.contains(path))
            .collect();
        assert!(
            missing.is_empty(),
            "under {root}, not packaged: {missing:?}"
        );
    }
    // and the one root that is a build output rather than a source tree, named
    // rather than counted, since `include_dir!` reads it at compile time and a
    // crate without it does not build
    for needed in ["ui/dist/index.html", "docs/README.md"] {
        assert!(packaged.contains(needed), "{needed} is not packaged");
    }
    assert!(
        packaged
            .iter()
            .any(|path| path.starts_with("ui/dist/assets/") && path.ends_with(".js")),
        "no built ui bundle is packaged"
    );
}

// every file `src/` reaches for at compile time, found by reading `src/` back
// rather than by keeping a list beside it. a page moved out of `docs/` or a
// new tree of fixtures under a root nothing ships is a compile error for a
// consumer and nothing here, which is the failure this test exists for.
#[test]
fn every_file_the_source_includes_is_packaged() {
    let packaged = packaged();
    let mut found = 0usize;
    for source in files_under("src") {
        if !source.ends_with(".rs") {
            continue;
        }
        let text = std::fs::read_to_string(root().join(&source)).expect("a source file");
        let dir = Path::new(&source).parent().expect("a parent").to_path_buf();
        for (macro_name, is_dir) in [("include_str!(\"", false), ("include_dir!(\"", true)] {
            for rest in text.split(macro_name).skip(1) {
                let literal = rest.split('"').next().expect("a closing quote");
                // `include_dir!` is given an absolute path through the one
                // variable cargo sets; `include_str!` is relative to its file
                let relative = literal.replace("$CARGO_MANIFEST_DIR/", "");
                let joined = if literal == relative {
                    dir.join(&relative)
                } else {
                    PathBuf::from(&relative)
                };
                let mut normal = PathBuf::new();
                for part in joined.components() {
                    if part.as_os_str() == ".." {
                        normal.pop();
                    } else {
                        normal.push(part);
                    }
                }
                let path = normal.to_string_lossy().replace('\\', "/");
                found += 1;
                let ships = if is_dir {
                    packaged.iter().any(|p| p.starts_with(&format!("{path}/")))
                } else {
                    packaged.contains(&path)
                };
                assert!(ships, "{source} includes {path}, which is not packaged");
            }
        }
    }
    // and something was found, so a rename of either macro cannot pass here by
    // checking nothing
    assert!(
        found > 5,
        "only {found} compile-time includes found in src/"
    );
}

/// the repository files the gate is written in twice. absent from the packaged
/// crate, which is a legitimate place to run this suite from, so these return
/// `None` there and the tests below assert nothing rather than failing on a
/// file that was never meant to ship.
fn repo_file(rel: &str) -> Option<String> {
    std::fs::read_to_string(root().join(rel)).ok()
}

/// which cargo invocations a line asks for, as (subcommand, feature set).
///
/// the toolchain token and any leading environment assignment are dropped, so
/// `cargo +nightly doc --all-features` and `cargo doc --all-features` are the
/// same entry: what is being compared is which configuration got run, not how
/// the line was spelled.
fn cargo_configs(text: &str) -> BTreeSet<(String, String)> {
    const SUBS: [&str; 5] = ["fmt", "clippy", "test", "check", "doc"];
    let mut found = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') || !line.contains("cargo") {
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let Some(start) = tokens.iter().position(|t| t.ends_with("cargo")) else {
            continue;
        };
        let Some(sub) = tokens[start + 1..]
            .iter()
            .find(|t| !t.starts_with('+') && !t.starts_with("\"+"))
            .filter(|t| SUBS.contains(&t.trim_matches('"')))
        else {
            continue;
        };
        let features = if line.contains("--all-features") {
            "all-features".to_string()
        } else if line.contains("--no-default-features") {
            "no-default-features".to_string()
        } else if let Some(rest) = line.split("--features ").nth(1) {
            format!("features {}", rest.split_whitespace().next().unwrap_or(""))
        } else {
            "default".to_string()
        };
        found.insert((sub.trim_matches('"').to_string(), features));
    }
    found
}

/// the gate is written down twice, in `ci.yml` and in the justfile, and only
/// one of them is what actually guards `main`. a justfile that has quietly
/// stopped covering a configuration prints the same green as one that covers
/// it, and the first anybody hears of the difference is a red push.
///
/// this is the same failure as the stale lists above: nothing about it shows
/// up in a compile or a clippy pass, so it is asserted.
#[test]
fn the_justfile_runs_every_configuration_ci_runs() {
    let (Some(ci), Some(just)) = (repo_file(".github/workflows/ci.yml"), repo_file("justfile"))
    else {
        return;
    };
    let missing: Vec<_> = cargo_configs(&ci)
        .difference(&cargo_configs(&just))
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "ci runs these and the justfile does not, so `just ci` is a weaker \
         gate than a push: {missing:?}"
    );
}

/// ci pins the msrv job to a literal version and the manifest declares one.
/// if they drift, the job is checking a rustc the crate never promised, in
/// whichever direction: too new and the promise is untested, too old and the
/// build fails for a version nobody claimed to support.
#[test]
fn the_msrv_ci_checks_is_the_msrv_the_manifest_promises() {
    let Some(ci) = repo_file(".github/workflows/ci.yml") else {
        return;
    };
    let declared = read("Cargo.toml")
        .lines()
        .find_map(|l| l.strip_prefix("rust-version = "))
        .map(|v| v.trim_matches('"').to_string())
        .expect("rust-version in Cargo.toml");
    let pinned: Vec<String> = ci
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- uses: dtolnay/rust-toolchain@"))
        .map(str::to_string)
        .filter(|t| t != "stable" && t != "nightly")
        .collect();
    assert_eq!(
        pinned,
        vec![declared.clone()],
        "ci pins {pinned:?} and Cargo.toml promises {declared}"
    );
}

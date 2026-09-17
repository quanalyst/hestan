//! Generic presentation metadata. Run and open the Jobs or Assets page;
//! select `label: collection` to project the same registrations another way.
use hestan::{Asset, Hestan, Job, Op};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    let job = Job::builder("refresh_a")
        .display_name("Refresh A")
        .group("alpha")
        .subgroup("shared")
        .label("collection", "one")
        .op(Op::new("step", |_| async { Ok(json!(1)) }))
        .build()?;
    let a = Asset::source("source_a")
        .display_name("Source A")
        .group("alpha")
        .subgroup("shared")
        .label("collection", "one");
    let b = Asset::source("source_b")
        .display_name("Source B")
        .group("beta")
        .subgroup("shared"); // A separate subgroup; this label value is missing.
    let result = Asset::new("combined", |_| async { Ok(json!(1)) })
        .display_name("Combined result")
        .group("alpha") // Clearly shown without a subgroup.
        .label("collection", "two")
        .from(&a)
        .from(&b);
    Hestan::new()
        .db(":memory:")
        .jobs([job])
        .assets([a, b, result])
        .serve(([127, 0, 0, 1], 4000))
        .await
}

//! the operator's binary: the same command line, with no registry of its own.
//!
//! it can do everything that only needs the run log (read runs, logs, events,
//! assets and schedules out of a database, pause things, move a run up the
//! queue) and everything a running instance will do on its behalf over the
//! http api. what it cannot do is launch anything on its own, because job
//! definitions are rust and this binary was not built from yours.
//!
//! that is the whole trade the [mount](hestan::cli) exists to avoid: put
//! `hestan::cli::run` in your own binary and every command works, because the
//! jobs are right there.

fn main() -> Result<(), hestan::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(hestan::cli::standalone())
}

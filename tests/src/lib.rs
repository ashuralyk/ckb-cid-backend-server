pub mod fake_rpc;
pub mod helpers;
pub mod host_query_syscalls;
pub mod process_resolver;
pub mod scripted_ssri;
pub mod submit_inflight;
pub mod vertical_batch_bounds;
pub mod vertical_confirm;
pub mod vertical_embedded_ssri;
pub mod vertical_failure;
pub mod vertical_fee;
pub mod vertical_filter;
pub mod vertical_incompatible;
pub mod vertical_n1;
pub mod vertical_reorg;
pub mod vertical_submit_conflict;
pub mod vertical_submit_drop;
pub mod vertical_submit_pending;
pub mod vertical_type_id_deps;

#[test]
fn contract_builders_default_to_the_workspace_revision() {
    let protocol = include_str!("../../Cargo.toml")
        .lines()
        .find(|line| line.starts_with("cid-protocol = "))
        .unwrap();
    let rev = protocol
        .split_once("rev = \"")
        .unwrap()
        .1
        .split('"')
        .next()
        .unwrap();
    let expected = format!("PIN=\"${{CID_DISPATCHER_REV:-{rev}}}\"");
    for (name, script) in [
        ("CID lock", include_str!("../../scripts/build-cid-lock.sh")),
        (
            "Process guest",
            include_str!("../../scripts/build-cid-process-example.sh"),
        ),
        (
            "local chain",
            include_str!("../../scripts/local-dev-chain.sh"),
        ),
    ] {
        assert!(
            script.lines().any(|line| line == expected),
            "{name} builder has a different default contract revision"
        );
    }
}

#[test]
fn observability_guide_lists_the_refund_events_emitted_by_the_worker() {
    use std::collections::BTreeSet;

    let guide = include_str!("../../docs/observability.md");
    let worker = include_str!("../../crates/cid-backend-core/src/process_batch.rs");
    let documented: BTreeSet<_> = guide
        .lines()
        .filter_map(|line| line.strip_prefix("| `refund."))
        .filter_map(|row| row.split_once('`').map(|(event, _)| event))
        .collect();
    let emitted: BTreeSet<_> = worker
        .match_indices("event = \"refund.")
        .filter_map(|(start, prefix)| worker[start + prefix.len()..].split_once('"'))
        .map(|(event, _)| event)
        .collect();

    assert_eq!(documented, emitted);
    assert!(!guide.contains("refund_fee_unaffordable"));
}

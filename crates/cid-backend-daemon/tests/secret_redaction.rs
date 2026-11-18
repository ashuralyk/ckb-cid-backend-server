//! Process-level regressions: synthetic secrets must not appear in daemon
//! stdout (JSON tracing) or stderr for config failures or refused local RPC.
//!
//! Uses only localhost sockets — no public-chain RPC.

use std::io::Read;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static TMP_N: AtomicU64 = AtomicU64::new(0);

const SECRET: &str = "SECRET_MARKER";
const TOKEN: &str = "TOKEN_MARKER";

fn daemon_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cid-backend-daemon"))
}

fn write_toml(body: &str) -> PathBuf {
    let n = TMP_N.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "cid-secret-redact-{}-{}.toml",
        std::process::id(),
        n
    ));
    std::fs::write(&p, body).expect("write toml");
    p
}

fn assert_no_secrets(label: &str, blob: &str) {
    assert!(!blob.contains(SECRET), "{label} leaked {SECRET}: {blob}");
    assert!(!blob.contains(TOKEN), "{label} leaked {TOKEN}: {blob}");
    assert!(
        !blob.contains("user:SECRET") && !blob.contains("user:s3cret"),
        "{label} leaked userinfo: {blob}"
    );
}

fn run_daemon_config(path: &std::path::Path) -> (i32, String, String) {
    let mut child = Command::new(daemon_bin())
        .arg("--config")
        .arg(path)
        .env_remove("CID_CONFIG")
        .env_remove("CID_CKB_RPC_URL")
        .env_remove("CID_INDEXER_URL")
        .env_remove("CID_LOCK_CODE_HASH")
        .env_remove("CID_POLL_INTERVAL_SECS")
        .env("RUST_LOG", "info")
        .env("CID_LOG_FORMAT", "json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon");
    let status = child.wait().expect("wait");
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut out) = child.stdout.take() {
        let _ = out.read_to_string(&mut stdout);
    }
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_string(&mut stderr);
    }
    (status.code().unwrap_or(-1), stdout, stderr)
}

fn free_localhost_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

#[test]
fn process_malformed_toml_omits_secrets_from_stdout_and_stderr() {
    let body = format!(
        r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://user:{SECRET}@127.0.0.1:8114/v1?api_key=prefix@{TOKEN}"
<<<
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = 1
"#,
        g = "11".repeat(32),
        lock = "22".repeat(32),
    );
    let path = write_toml(&body);
    let (code, stdout, stderr) = run_daemon_config(&path);
    assert_ne!(code, 0, "malformed config must exit non-zero");
    assert!(
        stdout.contains("config.load_failed") || stderr.contains("invalid config"),
        "expected load failure signal\nstdout={stdout}\nstderr={stderr}"
    );
    assert_no_secrets("stdout", &stdout);
    assert_no_secrets("stderr", &stderr);
}

#[test]
fn process_unknown_field_omits_secrets_from_stdout_and_stderr() {
    let body = format!(
        r#"
[network]
genesis_hash = "0x{g}"
private_key = "{SECRET}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = 1
"#,
        g = "11".repeat(32),
        lock = "22".repeat(32),
    );
    let path = write_toml(&body);
    let (code, stdout, stderr) = run_daemon_config(&path);
    assert_ne!(code, 0);
    assert_no_secrets("stdout", &stdout);
    assert_no_secrets("stderr", &stderr);
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        combined.contains("unknown field") || combined.contains("invalid config"),
        "expected unknown-field failure: {combined}"
    );
    assert!(
        !combined.contains("private_key"),
        "unknown field name must not appear in process output: {combined}"
    );
}

#[test]
fn process_typed_string_mismatches_omit_secrets_from_stdout_and_stderr() {
    let cases = [
        (
            "u64",
            format!(
                r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = "{SECRET}"
"#,
                g = "11".repeat(32),
                lock = "22".repeat(32),
            ),
        ),
        (
            "bool",
            format!(
                r#"
[network]
genesis_hash = "0x{g}"
validate_genesis = "{SECRET}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = 1
"#,
                g = "11".repeat(32),
                lock = "22".repeat(32),
            ),
        ),
        (
            "index",
            format!(
                r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[[cell_deps]]
tx_hash = "0x{dep}"
index = "{SECRET}"
[loop]
poll_interval_secs = 1
"#,
                g = "11".repeat(32),
                lock = "22".repeat(32),
                dep = "33".repeat(32),
            ),
        ),
    ];
    for (label, body) in cases {
        let path = write_toml(&body);
        let (code, stdout, stderr) = run_daemon_config(&path);
        assert_ne!(code, 0, "{label} must fail");
        assert_no_secrets(&format!("{label} stdout"), &stdout);
        assert_no_secrets(&format!("{label} stderr"), &stderr);
        let combined = format!("{stdout}\n{stderr}");
        assert!(
            combined.contains("invalid value type") || combined.contains("invalid config"),
            "{label}: expected typed diagnostic: {combined}"
        );
    }
}

#[test]
fn process_refused_local_rpc_omits_url_secrets_from_stdout_and_stderr() {
    let port = free_localhost_port();
    let body = format!(
        r#"
[network]
name = "local-redact"
genesis_hash = "0x{g}"

[rpc]
url = "http://user:{SECRET}@127.0.0.1:{port}/v1?api_key=prefix@{TOKEN}"
indexer_url = "http://user:{SECRET}@127.0.0.1:{port}/v1?api_key=prefix@{TOKEN}"

[cid_lock]
code_hash = "0x{lock}"
hash_type = "data1"

[loop]
poll_interval_secs = 1
confirmation_depth = 2

[observe]
log_format = "json"
"#,
        g = "aa".repeat(32),
        lock = "bb".repeat(32),
    );
    let path = write_toml(&body);

    let mut child = Command::new(daemon_bin())
        .arg("--config")
        .arg(&path)
        .env_remove("CID_CONFIG")
        .env_remove("CID_CKB_RPC_URL")
        .env_remove("CID_INDEXER_URL")
        .env_remove("CID_LOCK_CODE_HASH")
        .env_remove("CID_POLL_INTERVAL_SECS")
        .env("RUST_LOG", "info")
        .env("CID_LOG_FORMAT", "json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon");

    // First tick runs before poll sleep; allow settle then kill.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(_) => break,
        }
        // Enough for config.loaded + tip/scan unavailable on refused localhost.
        if Instant::now() >= deadline - Duration::from_secs(3) {
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();

    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    if let Some(mut out) = child.stdout.take() {
        let _ = out.read_to_end(&mut stdout_buf);
    }
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_end(&mut stderr_buf);
    }
    let stdout = String::from_utf8_lossy(&stdout_buf);
    let stderr = String::from_utf8_lossy(&stderr_buf);

    assert!(
        stdout.contains("config.loaded"),
        "expected successful config load\nstdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stdout.contains("tip.unavailable") || stdout.contains("scan.unavailable"),
        "expected refused-RPC tick event\nstdout={stdout}\nstderr={stderr}"
    );
    assert_no_secrets("stdout", &stdout);
    assert_no_secrets("stderr", &stderr);
    // Host may appear via redacted rpc= field; credentials/query must not.
    assert!(
        !stdout.contains(&format!("user:{SECRET}")),
        "stdout must not embed userinfo: {stdout}"
    );
    assert!(
        !stdout.contains(&format!("prefix@{TOKEN}")) && !stdout.contains(TOKEN),
        "stdout must not embed query token: {stdout}"
    );
}

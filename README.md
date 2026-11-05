# ckb-cid-backend-server

Public CID digest daemon: scan Intention cells, run Process SSRI in-process, submit execution transactions.

This repository is independent of [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher) (protocol, lock, `cid-process-executor`, `cid_process_example`).

**v0 design (approved):** [docs/superpowers/specs/2026-11-05-cid-backend-server-design.md](docs/superpowers/specs/2026-11-05-cid-backend-server-design.md)

Implementation has not started. First slice is the digest loop only (no HTTP API).

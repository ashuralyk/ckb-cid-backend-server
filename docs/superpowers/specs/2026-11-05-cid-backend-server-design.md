# CID Backend Server 设计 / Design

> **状态 / Status:** 已与 Lucas 在对话中拍板（2026-11-05）。本文只记录第一刀（v0）设计，不含实现代码。  
> **仓库 / Repo:** [ashuralyk/ckb-cid-backend-server](https://github.com/ashuralyk/ckb-cid-backend-server)  
> **上游协议 / Upstream:** [ashuralyk/ckb-common-intention-dispatcher](https://github.com/ashuralyk/ckb-common-intention-dispatcher)  
> **协议文 / Protocol:** [Common Intention Dispatch 协议设计](https://app.notion.com/p/3de9f5bf382080249b91d32ad72639d9)

本文对应协议里的 **公共意图执行服务**：扫描链上 Intention、按 Process 消化、提交 Execution Tx。仓库与 `ckb-common-intention-dispatcher` **独立**——那边是 CID lock、`protocol/`、`cid-process-executor`、`cid_process_example`；这边是消化循环。两边用 git 依赖与链上约定对接，不合并成一个 monorepo。

---

## 1. 一句话 / Goal

v0 做一个 **常驻 digest daemon**：按 indexer `asc` 扫 CID Intention，按 `process_type_hash` 分桶，进程内 SSRI（嵌入 `cid-process-executor`）取 batch、组 Execution Tx、提交节点。没有 HTTP / 运维面，没有持久化。重启等于从头重扫。

成功标准：FakeRpc 竖切覆盖 n=1、batch 边界、Incompatible 退回、提交冲突后下一 tick 重试；不连真实网络。

---

## 2. 范围 / Scope

### 2.1 做（v0）

- Workspace：`cid-backend-core` 库 + 薄 `cid-backend-daemon` 二进制；`tests/` 只依赖 core。
- 消化循环：scan → bucket → resolve Process 二进制 → SSRI batch → take N → assemble → submit。
- 正常路径 `digest_ok`：destroy Intention（每组 1 in / 0 out），Process **1 in / 1 out**，不追加独立 fee / sponsor input。
- 不兼容路径：SSRI `Incompatible` 时组 User Lock 退回（`digest_incompatible`）；组不出来则打日志并跳过。
- RPC 抽象为 Trait；测试用 FakeRpc。
- Process 二进制默认从链上 live Process Cell 的 data 解析；测试可注入 fallback 到 `cid_process_example` 二进制。

### 2.2 不做（v0 Non-goals）

| 不做 | 原因 |
|------|------|
| HTTP API、health、metrics、admin | 第一刀只有 daemon digest loop |
| Redis / SQLite / 任何磁盘状态 | 内存即可；重启全量重扫 |
| 独立 SSRI HTTP 服务 / 官方 ssri-server | 进程内嵌入 `cid-process-executor` |
| 多签 fee sponsor、额外 fee input | 交易已有 capacity 不够就跳过 |
| 用户再签一次 | Intention 创建后，`digest_ok` 不得把 User Lock 放进 inputs（那会走到 `timeout_burn`） |
| 超时 burn 代发 | 超时由 User Lock 自己发起；daemon 不走这条 hop |
| 生产级 Process recreate 打磨（超出 assemble 返回值） | skeleton 只保证 Process 1in/1out + assemble 给出的 outputs；不发明业务状态迁移 |
| 钱包 / CCC 接入、主网运营、多实例协调协议 | 协议允许竞争；v0 单进程、失败下一 tick 再扫 |

---

## 3. 已拍板决策 / Decisions

以下全部已定，实现不得改语义，只能改文件名与内部函数切分。

| # | 决策 | 选择 |
|---|------|------|
| D1 | 仓库 | 独立 `ashuralyk/ckb-cid-backend-server`，不并进 dispatcher |
| D2 | 第一刀 | **仅 daemon digest loop** |
| D3 | SSRI | **进程内**嵌入 `cid-process-executor`（ckb-vm + argv method id） |
| D4 | Process 二进制 | 生产：链上 live Process Cell data。测试：FakeRpc 解析失败时可注入 `cid_process_example` |
| D5 | 布局 | Approach A：pipeline 库 + daemon bin；RPC 为 Trait；FakeRpc 竖切 |
| D6 | 状态 | **仅内存**；进程退出即丢失；重启 = indexer 再扫一遍 live Intention |
| D7 | 用户签名 | `digest_ok` **不需要** Intention 用户再签；正确性交给 CID lock + Process 链上校验 |
| D8 | 手续费 | **不**另加 fee/sponsor input；吃交易里已有 capacity。不够则默认 **log + skip**（v0 **不**做开关；以后若加配置，默认仍是 skip） |
| D9 | 实现栈 | Rust workspace；SSRI 宿主与上游 `cid-process-executor` 对齐（Cinnabar 是上游合约工具链，本仓库不重新实现 VM） |

**否决的路线（记录，避免回潮）：**

- **Approach B（API-first）：** 先 HTTP 再循环。v0 没有调用方，只会拖慢 digest。
- **Approach C（外置 SSRI daemon）：** 多一个进程与部署面；与 D3 相反。
- **持久化游标：** 与 D6 相反；indexer `asc` + 全量 live 扫描足够。

---

## 4. 仓库布局 / Layout

```
ckb-cid-backend-server/
  Cargo.toml                          # workspace
  crates/cid-backend-core/            # 消化 pipeline（库）
  crates/cid-backend-daemon/          # 读配置、接真实 RPC、loop.run()
  tests/                              # FakeRpc 竖切；依赖 cid-backend-core
  docs/superpowers/specs/             # 本设计
```

| 构件 | 职责 | 不做什么 |
|------|------|----------|
| `cid-backend-core` | `rpc` / `scan` / `process` / `ssri` / `assemble` / `submit` / `loop` | 不读环境变量、不起 TCP、不 `main` |
| `cid-backend-daemon` | 配置（节点 URL、CID code hash、poll interval）→ 真实 RPC 实现 → `loop.run()` | 不含业务规则；规则全在 core |
| `tests/` | FakeRpc 宇宙 + 真实 `cid_process_example`（或注入的同等二进制） | 不访问公网 / 本地节点 |

Workspace 对上游的依赖（git，随实现锁定 revision）：

- `cid-process-executor` — `ProcessExecutor::get_batch_bounds` / `assemble`
- `cid-protocol` — `CidArgs`（`process_type_hash[32] \|\| molecule(CidEntry)`）

本仓库 **不** vendoring RISC-V 合约源码。测试二进制从上游 `build/release/cid_process_example` 取得（文档说明如何构建），或由 FakeRpc 的 process fallback 指向同一文件。

---

## 5. 核心模块 / Core modules

全部在 `cid-backend-core`。模块之间只通过下面写明的数据走；`loop` 是唯一编排者。

### 5.1 `rpc` — 链访问 Trait

Trait 覆盖 daemon 与 FakeRpc 共同需要的三类能力：

1. **Indexer 扫描：** 按 CID lock `code_hash`（及配置中的 hash type，默认与协议一致为 Type）列出 live Intention cells；顺序必须是 indexer **`asc`**。
2. **Live cells / Process data：** 按 type script hash 取 live Process Cell；读 cell data（合约二进制）、capacity、lock/type、outpoint。
3. **Submit：** 把组好的交易交给节点（FakeRpc 则写入内存链并做冲突检测）。

真实实现走 CKB node + ckb-indexer JSON-RPC。FakeRpc 是内存 cell 宇宙，行为对齐上游 dispatcher 测试：可预置 Intention / Process、模拟双花、不发起网络。

v0 Trait **不**包含：钱包、subscribe、批量 debug dump。

### 5.2 `scan`

- 用 Trait 拉 live Intention。
- 只接受 lock code hash（及 hash type）匹配配置的 CID 的 cell。
- 解析 lock args：`CidArgs::decode` → `process_type_hash` + `CidEntry { version, user_lock, payload }`。
- 解码失败：打日志，**跳过该 cell**，不中断本 tick。
- 输出：有序列表（保持 `asc`），每项带 outpoint、capacity、type（纯 CKB 或 xUDT）、data（Intention data 在 v0.1 为金额或空）、解码后的 args。

本模块不调 SSRI、不组交易。

### 5.3 `process` — 解析 Process 二进制

- 输入：某个 `process_type_hash`。
- 生产路径：Trait 查找 **type script hash 等于该值** 的 live Process Cell，取其 **data 作为 RISC-V 二进制**（协议：Process data = 合约二进制；Type ID 保证该 hash 链上唯一）。
- 找不到、data 空、明显不是可执行 guest：打日志，该 bucket **本 tick 跳过**（不 abort 整个 tick）。
- **测试注入：** `ProcessResolver` 可带 fallback 闭包/路径；FakeRpc 竖切在「链上 data 不是完整 guest」时落到 `cid_process_example`。生产 daemon **不安装**该 fallback。

解析结果缓存仅限 **当前进程、当前 tick 或同一次 loop 生命周期内的内存 map**（hash → binary + live cell meta）。重启即丢。不把二进制写盘。

### 5.4 `ssri`

对 `cid-process-executor::ProcessExecutor` 的薄封装：

- `batch(binary) -> (min, max)` 对应 SSRI `Process.batch`（样例 Process 为 `(1, 4)`；daemon **不得写死** 1/4，以 guest 返回为准）。
- `assemble(binary, intentions) -> Result<PartialTx, SsriError>` 对应 `Process.assemble`。

错误映射（与上游 executor 对齐，语义固定）：

| Executor / SSRI | 本模块 | 循环怎么处理 |
|-----------------|--------|----------------|
| `ExecutorError::Incompatible` | `SsriError::Incompatible` | 走 User Lock 退回路径 |
| VM / cycles / invalid response | `SsriError::Unusable` | 打日志，该 batch **跳过**（不当成 Incompatible，避免误退款） |

v0 不解释 payload；payload 原样交给 guest。

### 5.5 `assemble`

把 SSRI 输出变成可提交的 Execution skeleton。

**`digest_ok`（默认）：**

1. Intention **全部作为 inputs**，且每个 CID lock group **1 in / 0 out**（destroy，不可部分消化）。
2. 把 `assemble` 返回的 molecule `Transaction` 里的 outputs / outputs_data 接到 skeleton（业务受益人等）。
3. 把 **当前 live Process Cell** 做成 **1 in / 1 out**：input 即解析到的那颗 cell；output 至少保持 type script 连续（hash 仍等于 Intention 绑定的 Process Type Hash）。output 的 lock/capacity/data 能用 assemble 结果就用；assemble **没**给出 Process recreate 时，v0 **原样复制** live cell 的 lock / data / capacity / type（只换新 outpoint）。这就是「不超出 assemble 返回值做生产级 recreate 打磨」。
4. **不**增加单独 fee/sponsor input。
5. **不**把 Intention 的 User Lock 放进 inputs。

**`digest_incompatible`：**

1. 仍 destroy Intention（1 in / 0 out）。
2. **不得**出现匹配的 Process 对（否则 CID 会优先 `digest_ok`）。
3. 第一个给 User Lock 的 output 必须是 **精确退回**：原 capacity；若有标准 xUDT 则 type hash 与金额不变。
4. User Lock **不得**出现在 inputs。
5. 同样不加 fee input。

Capacity 检查：在提交前算 `sum(inputs.capacity) - sum(outputs.capacity)`。若为负，或节点侧会因无 fee input 而不可接受：打日志并 **skip**（D8）。零手续费且节点接受时允许提交。v0 不为退回「再垫一笔 fee cell」。

代码 cell / CellDeps：执行 Process lock/type 所需的 code deps 由 assemble 模块按 live Process 与已知 CID/xUDT 依赖补齐。这与「live Process 是 input 不是 CellDep」不冲突：状态 cell 走 IO，code 走 CellDeps。

若退回路径缺 User Lock、缺 xUDT type、或无法在不加 fee input 的前提下做出精确退回：打日志，**跳过**，不提交残缺交易。

### 5.6 `submit`

- 把 skeleton 交给 `rpc.submit`。
- 成功：该批 Intention 视为本轮完成（链上应已消失；下一 tick 扫描自然没有）。
- 失败（含双花 / cell 已被别人消耗）：**不写盘**；这些 outpoint 留在内存的 in-flight / 失败集，供下一 tick 判断；cell 若仍 live，下一 tick 扫描会再次看到并重试。
- 不在 submit 模块里改交易重签。失败即整笔丢弃，下一轮从 scan 重来。

### 5.7 `loop`

编排 **一个 tick**，然后按配置的 poll interval sleep，再 tick。全部可变状态在 `LoopState`（内存）：

- 可选：本轮已提交、等待 indexer 追上的 outpoint 集合，避免同一轮重复提交。
- Process 二进制内存缓存。
- 无游标文件、无「已处理高度」持久化。

`run()`：`loop { tick(); sleep(interval) }`，直到进程被杀。单 tick 错误策略见 §7：默认继续下一 bucket / 下一 Intention，**不**因单条坏数据退出进程。

测试用可注入的 clock / 只跑 `tick()` 一次，避免真实 sleep。

---

## 6. Daemon

`cid-backend-daemon` 是薄包装：

1. 读配置（v0 **只读环境变量**，不引入配置文件或 CLI flag）：
   - **节点 URL**（CKB RPC）。若 indexer 与节点不同址，再读可选的 indexer URL；未设则与节点 URL 相同。
   - **CID lock code hash**（必填）。CID hash type 默认 Type，可用可选变量覆盖。
   - **poll interval**（必填时长；测试注入假时钟，不在 spec 里写死毫秒数）。
2. 构造真实 RPC。
3. 构造 **无** Process fallback 的 resolver。
4. `loop.run()`。

不提供 REST、不提供「手动 digest 这一笔」CLI（debug 用 `RUST_LOG` 即可）。密钥：v0 **不为 User Lock 配密钥**。若 Process cell 的 lock 需要操作者签名，daemon 只在配置了对应解锁材料时才能 `digest_ok`；未配置则该 bucket skip。FakeRpc 测试使用可解锁 / always-success 的 Process lock。

---

## 7. 数据流与错误 / Data flow and errors

### 7.1 一个 tick

```
scan (CID code hash, asc)
  → decode args；坏 args 则 log/skip
  → bucket by process_type_hash（桶内保持 asc）
  → 每个 bucket:
       resolve live Process binary（失败则 log/skip 该桶）
       ssri.batch → (min, max)
       从桶头取 N 条，N ∈ [min, max]（活 cell 不足 min 则本桶 skip，等下一 tick）
       ssri.assemble
         · Ok    → assemble digest_ok skeleton（Intention inputs + assemble outputs + Process 1in/1out）
         · Incompatible → assemble 退回；组不出则 log/skip
         · 其他 SSRI 错 → log/skip（不退款）
       capacity 不够（无 fee input）→ log/skip
       submit
         · Ok → 记下 in-flight
         · 冲突/拒绝 → 留在内存，下一 tick 再扫到再试
```

同一 tick 内不同 `process_type_hash` 顺序：按本 tick 首次出现顺序（即全局 `asc` 里该 hash 第一次出现的位置）。同一 Process 的多批：取完一批提交后再取该桶剩余，仍遵守 batch 上下界。

### 7.2 错误表（不得 abort 整个 tick，除非 RPC 配置彻底不可用）

| 情况 | 行为 |
|------|------|
| 单条 Intention args 解析失败 | log + skip 该 cell |
| 无 live Process / data 不可用 | log + skip 该 bucket |
| SSRI `Incompatible` | 尝试 User Lock 退回；组不出则 log + skip |
| SSRI VM / invalid | log + skip 该 batch，**不**退款 |
| 活 Intention 数 < `batch.min` | skip 该桶（等更多意图或等下一 tick） |
| 活 Intention 数 > `batch.max` | 只取前 `max` 条（仍是 asc 桶头） |
| Cell 竞争 / tx 被拒 | 内存记下失败；下一 tick 重试 |
| `sum(in) < sum(out)` 或无 fee input 导致无法上链 | log + skip（D8） |
| 节点 / indexer 本 tick 不可达 | log；sleep 后下一 tick 再试；不写半成品到盘 |

进程仅在配置非法（缺 URL / 缺 CID code hash）时启动失败退出。

### 7.3 与 CID hop 的对应

交易结构推断（与上游 lock 一致），daemon 只构造前两条：

| Hop | Daemon 是否构造 | 结构要点 |
|-----|-----------------|----------|
| `digest_ok` | 是 | Process type hash 同时在 inputs 与 outputs；无 User Lock input |
| `digest_incompatible` | 是（仅 SSRI Incompatible） | 无匹配 Process 对；User Lock 退回 output；无 User Lock input |
| `timeout_burn` | **否** | 需要 User Lock input + header deps；留给用户 |

---

## 8. 状态模型 / State

| 状态 | 位置 | 生命周期 |
|------|------|----------|
| Live Intention / Process | 链 | 扫描时读取 |
| Process 二进制缓存 | 进程内存 | 随进程；可不跨 tick 持久 |
| In-flight / 刚失败的 outpoint | 进程内存 | 避免 indexer 延迟导致重复提交；重启后为空 |
| 已消化 Intention | 无本地记录 | 以链为准 |

重启后：缓存与 in-flight 为空，下一 tick 全量 `asc` 扫描。可能与「已广播但未进 indexer」窗口重叠——重复提交应由节点以双花拒绝，再走 §7.2 竞争重试。不为此做磁盘 WAL。

---

## 9. 测试 / Testing

第一刀 **禁止** live network 测试。

### 9.1 FakeRpc 竖切（`tests/`）

共用 FakeRpc：内存 cells、`submit` 可注入冲突。Process 二进制走注入的 `cid_process_example`（或 FakeRpc cell data 已是该二进制）。

必须覆盖：

1. **n=1：** 一笔合法 Intention → `digest_ok` 形状（Intention destroy、Process 1in/1out、无 fee input、无 User Lock input）且 FakeRpc 接受。
2. **batch 边界：** 低于 `min` 不提交；等于 `max` 提交；超过 `max` 只吃前 `max` 条。
3. **Incompatible 退回：** guest/executor 返回 Incompatible → 链上出现 User Lock 精确退回，且无 Process 对。
4. **提交冲突再重试：** 第一次 `submit` 失败（cell 已被占）→ 状态仍在内存 → 下一 `tick` 在 cell 仍 live 时再次提交并成功。

### 9.2 单元测试（`cid-backend-core`）

- `scan`：CID args 解码；坏 args 跳过；`process_type_hash` 分桶且保持 asc。
- `assemble`：`digest_ok` **没有** fee/sponsor input；Process 1in/1out；User Lock 不在 inputs。

不测超时 hop、不测真实 secp 签名广播（那是上游 lock 仓库的范围）。

---

## 10. 与上游的边界 / Boundary with the lock repo

| 在 dispatcher 仓库 | 在本仓库 |
|--------------------|----------|
| CID lock、Verify、TIMEOUT_BLOCKS | 不复制合约源码 |
| `cid_process_example` 合约 | 测试用其 **构建产物**，不当作生产 Process |
| `cid-process-executor` | **嵌入**为 SSRI 宿主 |
| `calculator/process` 的 `process_digest_ok` | 不强制调用；本仓库 `assemble` 自己补 Process 1in/1out（上游 calculator 仍可能未 compose recreate） |
| FakeRpc 测 lock 语义 | FakeRpc 测 **执行服务 pipeline** |

协议不变量本仓库必须遵守：完整 destroy、不可部分消化、indexer asc、按 Process Type Hash 分发、batch 来自 SSRI、Incompatible → User Lock 退回、Type ID 唯一 Process、正确性在链上。

---

## 11. 实现顺序（给后续 plan 用，本文不展开任务）

设计已闭合。下一步应另写 `docs/superpowers/plans/` 实现计划，建议顺序：core Trait + FakeRpc → scan/decode/bucket → process resolver + fallback → ssri wrap → assemble（无 fee input + Process 1in/1out）→ submit/retry → loop → 薄 daemon。每步先测后码。在用户确认本 spec 之前 **不写实现代码**。

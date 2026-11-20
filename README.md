# 离线通信接收端模拟器

完全离线的通信接收端模拟器：只有合成的报文元数据、模拟认证结果与本地
SQLite 持久化，**没有任何网络监听**。Windows 原生 Python 3.12、仅标准库、
CPU 环境；全部输入自行合成，全部测试离线运行。

## 运行

```bash
python -m solution --input cases.json --output result.json
python -m unittest discover -s tests -v
```

## 目录结构

```
solution/
  __main__.py   # CLI：--input cases.json --output result.json
  core.py       # 接收端核心：窗口恢复、回执绑定、撤权空位、版本化推进
  runner.py     # op 执行器（CLI 与 worker 共用）
  worker.py     # 可自杀的工作进程（崩溃恢复测试用）
  writer.py     # 独立写进程（并发/授权测试用）
cases.json      # 可重放输入（9 个场景）
result.json      # 上述输入的实测输出
tests/
  reference_model.py   # 独立定长窗口参考模型（纯内存、独立实现）
  test_core.py         # 核心规则单测
  test_cli.py          # CLI 端到端具体断言
  test_crash.py        # 两个崩溃点的终止-重启-重放
  test_concurrent.py   # 独立写进程并发/授权场景
  test_reference.py    # 随机合成序列 + 完整前后边界的参考回放对拍
```

## 规则

1. **会话建立**：`establish` 固定租户（tenant）、连接代数（conn_gen）与
   首个已知绝对序号（base_seq），`expected = base_seq`。以更大的
   conn_gen 重新建立视为重连（全新状态，旧代回执归档保留）；重用旧的
   或当前的连接号被拒绝（`ERR_CONN_GEN`）。
2. **报文**：携带 16 位循环序号（0..65535）、连接代数、密钥代数、数据
   摘要，外加模拟认证结果 `auth`。
3. **序号恢复**：以 `expected` 为中心，仅在两侧各 128 项窗口
   `[expected-128, expected+128)` 内恢复唯一绝对序号。窗口左半仅用于
   核查历史回执；右边界及之外拒绝（`ERR_WINDOW_RIGHT`）；左边界之外
   拒绝（`ERR_WINDOW_LEFT`）；序号恰在半圈（距离 32768）处不得推断
   （`ERR_HALF_CYCLE`）。绝对序号单调递增，跨越 65535→0 回绕。
4. **密钥代数**：每个密钥代数登记一个以绝对序号限定的有效区间
   `[lo, hi]`（`hi` 可空表示开放）。旧新密钥区间可重叠、同时有效。
   同一绝对序号最多接收一次：首次接收记录持久绑定摘要与密钥证据，
   之后同序号报文与回执比对——证据一致为 `duplicate`（按持久回执应
   答），不一致为 `ERR_CONFLICT`。
5. **乱序缓存与连续释放**：窗口内的合法报文持久缓存（state=cached），
   只从 `expected` 起连续释放到队列（state=delivered）；空位
   （state=gap）被跳过。
6. **授权撤销**：`revoke` 只让该代**仍未交付**的缓存数据失效，并为每
   个这样的序号写显式空位；已交付的历史不撤销。被撤代的密钥再使用
   报 `ERR_KEY_REVOKED`；落在空位上的报文报 `ERR_GAP`。
7. **版本化推进**：所有推进在 `BEGIN IMMEDIATE` 事务内完成，提交前以
   `UPDATE ... WHERE version=?` 的事务内版本为准（乐观并发），冲突
   则回滚重试。认证失败与一切拒绝都不落盘、不推进版本。
8. **崩溃恢复**：报文保存与队列推进同事务提交；回复在提交之后写出。
   进程在"已保存未提交"处死亡 → 事务回滚，重放后重新接收；在"已提
   交未回复"处死亡 → 重放时由持久回执应答 duplicate，不会重复入队。

## 输入格式（cases.json）

```json
{"cases": [{"name": "...", "ops": [ <op>, ... ]}]}
```

op 种类：

| op | 字段 | 说明 |
|---|---|---|
| `establish` | `session, tenant, conn_gen, base_seq` | 建立会话（固定租户/连接代数/首个已知绝对序号） |
| `key` | `session, key_gen, lo, hi?` | 登记密钥代数的绝对序号有效区间，`hi` 可空 |
| `recv` | `session, seq, conn_gen, key_gen, digest, auth?, tenant?` | 合成报文；`seq` 为 16 位循环序号；`auth` 默认 true |
| `revoke` | `session, key_gen` | 撤销密钥代数 |

## 输出格式（result.json）

```json
{"input": "cases.json",
 "cases": [{"name": "...",
            "ops": [{"i": 0, "op": "recv", "status": "...", ...}],
            "sessions": {"<sid>": {"state": {...},
                                   "delivered": [{"abs_seq":..,"digest":..,"key_gen":..}],
                                   "gaps": [...], "cached": [...]}}}]}
```

`recv` 的 `status`：

- `delivered`：本次接收并连续释放（附 `released` 绝对序号列表）
- `cached`：合法乱序报文，已持久缓存
- `duplicate`：与首次接收回执证据一致的重传（附 `receipt_state`）
- `rejected`：拒绝，附 `code` 与 `error`

## 错误码

| code | error | 含义 |
|---|---|---|
| 1001 | ERR_AUTH | 模拟认证失败，无任何落盘 |
| 1002 | ERR_SESSION_UNKNOWN | 会话不存在 |
| 1003 | ERR_CONN_GEN | 连接代数不符（旧连接号重用） |
| 1004 | ERR_TENANT | 租户与会话不符 |
| 1005 | ERR_SEQ_RANGE | 线序号不在 0..65535 |
| 1006 | ERR_HALF_CYCLE | 恰在半圈处，不得推断 |
| 1007 | ERR_WINDOW_LEFT | 超出窗口左边界 |
| 1008 | ERR_WINDOW_RIGHT | 达到/超出窗口右边界 |
| 1009 | ERR_KEY_UNKNOWN | 密钥代数未登记 |
| 1010 | ERR_KEY_REVOKED | 密钥代数已撤销 |
| 1011 | ERR_KEY_INTERVAL | 密钥在该绝对序号上无效 |
| 1012 | ERR_CONFLICT | 同序号但摘要/密钥证据与首次回执不符 |
| 1013 | ERR_GAP | 该序号已被撤权置为显式空位 |
| 1014 | ERR_SESSION_EXISTS | 同代重复建立且参数不同 |
| 1015 | ERR_KEY_EXISTS | 密钥代数重复登记 |
| 1016 | ERR_NO_RECEIPT | 窗口左半内但无历史回执 |

## 崩溃与并发测试设计

- `solution.worker` 支持 `--crash OP:pre_commit`（乱序报文已在事务内保
  存、COMMIT 前自杀，回滚）与 `--crash OP:post_commit`（队列推进已提
  交、回复写出前自杀）。`tests/test_crash.py` 对两个点分别终止工作
  进程、重启重放同一脚本，断言有序交付 [100..105]、拒绝原因稳定
  （ERR_WINDOW_RIGHT/ERR_AUTH）、持久回执每序号仅一条、队列无重复。
- `solution.writer` 是独立写进程。`tests/test_concurrent.py` 覆盖：
  两个写进程以两个密钥同时入队同一序号（恰好一个获胜，另一个全部
  ERR_CONFLICT，回执仅一行）；前序缺失后撤权（201/202 缓存→撤权置空
  位→200 补齐后 expected=203）；65535→0 交接（delivered
  [65534..65537]）；旧连接号重用（ERR_CONN_GEN 且新代无入队）；失效
  认证无入队（receipts 为空、version=0）。
- `tests/test_reference.py` 用独立编写的定长窗口参考模型对拍：6 轮
  ×1500 步随机合成序列（含乱序、回绕、半圈、越窗、冲突摘要、随机撤
  权、失效认证）逐步比对状态/错误码/释放列表，并对 ±128 窗口边界、
  ±32768 半圈、65535→0 回绕做完整前后边界扫描。

## 实测结果（本机，Windows 11，Python 3.12.8，SQLite 3.45.3）

```
python -m solution --input cases.json --output result.json   # 退出码 0
python -m unittest discover -s tests -v
Ran 36 tests in ~1.9s
OK
```

`cases.json` 九个场景的实测摘要（见 `result.json`）：

| 场景 | 交付 | 空位 | 拒绝 |
|---|---|---|---|
| in_order_delivery | 1000,1001,1002 | – | – |
| out_of_order_contiguous_release | 100..104（100 到达后释放 100-102，103 到达后释放 103-104） | – | – |
| wrap_65535_to_0 | 65534,65535,65536,65537 | – | – |
| window_boundaries | – | – | +128→ERR_WINDOW_RIGHT，左窗无回执→ERR_NO_RECEIPT，-129→ERR_WINDOW_LEFT，半圈→ERR_HALF_CYCLE |
| dual_key_overlap_and_conflict | 100 | – | 异密钥同序号→ERR_CONFLICT，区间外→ERR_KEY_INTERVAL |
| revoke_with_missing_predecessor | 200,201,204,205 | 202,203 | 撤代密钥→ERR_KEY_REVOKED，空位→ERR_GAP |
| connection_generation_reuse | 5000（新代） | – | 旧连接号报文/重建→ERR_CONN_GEN |
| invalid_auth_no_enqueue | 300 | – | 两次 ERR_AUTH，无落盘 |
| receipt_binding_and_duplicates | 700 | – | 异摘要同序号→ERR_CONFLICT；重传→duplicate |

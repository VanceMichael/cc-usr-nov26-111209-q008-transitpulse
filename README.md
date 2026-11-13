# transitpulse

公交实时接口 JSON 表示的实体查看器、源响应档案与实时源接收状态库。

## 现有能力

- **单包观察（保留）**：`inspect` 查看一份源文件的结构，不改变任何状态。
- **原文档案（保留）**：`capture` 发起一次本机 HTTP 请求并保存响应状态、原始字节和摘要；`show` 列出接收记录。原文对象按 SHA-256 共用，独立请求分别记账。错误 HTTP 状态也会保存，原文归档不等于业务接收完成。
- **实时源接收（新增）**：把供应商 JSON 形式的 GTFS-Realtime 子集沉淀为可按运营日查询的班次现况，业务范围只到数据采集，不做路线规划。
  - FULL_DATASET 缺席失效、DIFFERENTIAL 缺席保留、显式 `is_deleted` 三种语义分开记账；
  - TripUpdate 与 VehiclePosition 作为同一实体的两个 aspect 独立存储，各自更新时刻互不冲销；
  - 缺 `start_date` 时按来源时区与超 24:00 静态时刻推导候选，多候选保持未绑定，不挑最近一班；
  - `arrival` 缺失与 `arrival.time=0` 分别保留，NO_DATA / SKIPPED / CANCELED 不折算为普通延误；
  - feed 重放、较旧 header、同 header 异文冲突、接收事务中断都有独立 receipt 可解释；
  - 静态表换版时冻结旧版快照并重绑，单个实体重绑失败不清空整条线，旧版结果继续可查。

## 运行

使用 Python 3.11 或更新版本，仅依赖标准库，无安装步骤。全部示例均为本地合成资料，不包含真实个人信息、商业凭据或线上地址。

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q transitpulse
python3 -m transitpulse inspect transitpulse/fixtures/feed.json
python3 -m transitpulse demo
```

### 手工复现班次日视图的变化

需要手动发请求时运行 `python3 -m transitpulse.source_server --port 8765`，再在另一终端执行下列命令（默认在当前目录生成 reception.sqlite3、captures.sqlite3 与 objects）：

```sh
# 1. 绑定第一版静态表
python3 -m transitpulse load-static transitpulse/fixtures/static-v1.json
# 2. 接收首个完整 feed（trip1 + vehicle1，含 arrival.time=0 与 NO_DATA）
python3 -m transitpulse ingest transitpulse/fixtures/feed.json
# 3. 只更新车辆的差分包：trip1 的 TripUpdate 必须仍在
python3 -m transitpulse ingest transitpulse/fixtures/difference.json
# 4. 显式删除 trip1
python3 -m transitpulse ingest transitpulse/fixtures/deleted.json
# 5. 换静态表并重绑
python3 -m transitpulse load-static transitpulse/fixtures/static-v2.json
# 6. 缺 start_date 的歧义完整包：实体保持未绑定并列出候选
python3 -m transitpulse ingest transitpulse/fixtures/ambiguous.json
# 7. 班次日视图：20261111 服务日的 25:10 与当日 01:10 并列
python3 -m transitpulse day 20261112
# 8. 旧版静态表结果继续可查
python3 -m transitpulse day 20261112 --version static1
# 9. 收据、版本绑定与实体追溯
python3 -m transitpulse receipts
python3 -m transitpulse versions
python3 -m transitpulse trace unspecified
```

`day 20261112` 输出中：`trips` 给出与该当地日历日有交集的班次（含前一服务日跨午夜班次）及绑定 aspect；`unbound_entities` 列出未绑定实体与候选；`removed_entities` 列出失效实体及删除证据；`bound_feed` 标明当前页面绑定的 feed receipt 与 header 时间戳。每个 aspect 的 `receipt_id` 可经 `receipts` 查到 `capture_id`，再由 `trace` 回到 Archive 中的原始 FeedEntity。

也可以用 `ingest-url http://127.0.0.1:8765/feed` 直接从本机源抓取并接收（先归档原文再推进状态）。`demo` 命令自己启动源服务，使用随机本机端口，并在退出时清理临时目录；除上述主流程外还会演示重放、旧 header、内容冲突与事务中断的 receipt。

## 模块

| 模块 | 职责 |
| --- | --- |
| `transitpulse/parser.py` | 单文档结构观察（原有，不持有状态） |
| `transitpulse/realtime.py` | feed 校验、实体规范化、班次绑定判定 |
| `transitpulse/schedule.py` | 静态班次表、超 24:00 时刻与候选区间推导 |
| `transitpulse/reception.py` | 接收事务、增量语义、删除证据、换版重绑、日视图 |
| `transitpulse/scenarios.py` | 可复现的端到端演练编排（demo 与测试共用） |
| `transitpulse/archive.py` | 不可变原文与请求事实 |
| `transitpulse/transport.py` | 限制实验请求到本机 |
| `transitpulse/source_server.py` | 来源示例 |

来源边界及字段含义见 `docs/source-contract.md`。这些入口不会声称一次响应已经构成完整目录或业务批次。

## 容器

Dockerfile 使用 Python 3.11 标准库镜像，不需要应用依赖下载。slim 镜像不附带 tzdata，`schedule.py` 对演练时区内置固定偏移回退，容器内行为与本地一致。可用 `docker build -t transitpulse:local .` 构建，再执行 `docker run --rm --network none transitpulse:local`。本地 HTTP 与 PTY 演练不需要外部网络，测试及单次演示通常数秒完成。

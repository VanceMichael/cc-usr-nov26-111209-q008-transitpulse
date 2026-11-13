# 实时公交来源

GET /feed 为供应商约定的 GTFS-Realtime 2.0 JSON（snake_case 字段），不是 Protobuf wire 服务。header.timestamp 是 UTC Unix 秒；incrementality 缺省 FULL_DATASET；实体id只在同来源内稳定。显式 is_deleted 删除的是该 feed 实体，不删除静态班次；FULL_DATASET 中缺席实体失效，DIFFERENTIAL 未携带实体继续有效。实体各自 timestamp 与 header 不可相互替代。同 id 同 header timestamp 异文为冲突；较旧 header 不推进当前状态，原文可留存。

GET /static 给出已展开有效服务日的静态班次，不要求解读完整 GTFS zip。start_time/end_time 可超过24小时，日期按 timezone 的当地午夜加时长解释。缺 start_date 时，候选为 trip_id 相同、按各服务日起止时刻覆盖实体 timestamp 的班次；若有 start_time 还须匹配对应服务日内时长。精确给出 start_date 则按 descriptor 匹配，多个候选都不可任意挑选。arrival 对象缺失与 time=0必须保留，NO_DATA、SKIPPED、CANCELED 不折算为普通延误。

/control/phase?value=difference 返回只包含 vehicle 的差分包；value=deleted 删除 trip 实体；value=ambiguous 换静态表并给出缺服务日实体。供应商单次完整 feed 接收后才形成新当前状态。TripUpdate 与 VehiclePosition 关联同一班次但不共享更新时间字段。全部车辆与站点都是演练数据。

## 接收端如何落实这些语义

`transitpulse/realtime.py` 只做结构校验与规范化，`reception.py` 负责状态推进，两者都不做路线规划，业务边界到数据采集为止。

### 三种“缺席”分开记账

1. **FULL_DATASET 缺席**：整包接收成功后，包内未出现的有效实体整体失效，事件记 `absent_in_full_dataset`；
2. **DIFFERENTIAL 缺席**：差分包没带到的实体继续有效，旧脚本“增量回应里没出现就删除”的行为在此明确否定；
3. **显式 is_deleted**：独立证据，事件记 `is_deleted`，与 FULL_DATASET 缺席不混用。

### 两个 aspect 各自独立

TripUpdate 与 VehiclePosition 是同一 FeedEntity 的两个 aspect，分别存 `entity_aspects` 行、各自带 `aspect_ts`。一个 aspect 收到更旧的更新时刻只跳过自己（事件 `stale_aspect_skipped`），不会冲掉另一 aspect，也不会用 header timestamp 顶替实体自己的时刻。

### 班次绑定不猜

- descriptor 给出 start_date：按 trip_id + service_date（+start_time）精确匹配；
- 缺 start_date：用来源时区把静态班次换算成绝对区间 `[start_ts, end_ts)`，取覆盖实体时刻的候选；start_time 若存在还要与服务日内时长一致；
- 候选为 0（`no_candidate`）或多于 1（`ambiguous`）都保持 `unbound`，候选列表原样保留，绝不挑“最近的一班”；
- descriptor 缺失、没有任何可用时刻、静态表缺失分别记 `no_descriptor`、`no_timestamp`、`no_schedule`。

### 接收事务与可解释收据

每份 feed 在单个 SQLite 事务内应用，任何实体处理失败则整包回滚，当前状态不变，同时写一条 `status=failed` 的 receipt。receipt 状态集合：`applied / replayed / stale / conflict / rejected / failed`。同 digest 重放为 `replayed`；header 较旧为 `stale`；同 header timestamp 异文为 `conflict`；结构非法为 `rejected`。原文归档（Archive）与业务接收（Receipt）分开，HTTP 归档成功不等于业务接收完成。

### 静态表换版

`load-static` 载入新版本时先把旧版的全部有效/失效 aspect 冻结到 `revision_snapshots`，再对当前有效 aspect 逐个重绑。单个 aspect 重绑异常只标记 `unbound/rebind_error` 并记事件，不回滚整条线路。旧版日视图经 `day --version` 继续可查，未绑定实体的候选按冻结的旧版静态班次重新推导。

### 班次日视图的追溯链

`day YYYYMMDD` 的每个 aspect 带 `receipt_id`，receipt 带 `capture_id`，capture 指向 Archive 中按 SHA-256 存储的原始字节；`trace 实体id` 汇总该实体的全部事件与对应原文实体。视图中的 `removed_entities` 保证失效车辆仍可见且附删除证据，`unbound_entities` 保证歧义车辆显示候选而不是凭空消失。

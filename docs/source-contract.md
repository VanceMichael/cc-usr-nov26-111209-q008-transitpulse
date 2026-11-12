# 实时公交来源

GET /feed 为供应商约定的 GTFS-Realtime 2.0 JSON（snake_case 字段），不是 Protobuf wire 服务。header.timestamp 是 UTC Unix 秒；incrementality 缺省 FULL_DATASET；实体id只在同来源内稳定。显式 is_deleted 删除的是该 feed 实体，不删除静态班次；FULL_DATASET 中缺席实体失效，DIFFERENTIAL 未携带实体继续有效。实体各自 timestamp 与 header 不可相互替代。同 id 同 header timestamp 异文为冲突；较旧 header 不推进当前状态，原文可留存。

GET /static 给出已展开有效服务日的静态班次，不要求解读完整 GTFS zip。start_time/end_time 可超过24小时，日期按 timezone 的当地午夜加时长解释。缺 start_date 时，候选为 trip_id 相同、按各服务日起止时刻覆盖实体 timestamp 的班次；若有 start_time 还须匹配对应服务日内时长。精确给出 start_date 则按 descriptor 匹配，多个候选都不可任意挑选。arrival 对象缺失与 time=0必须保留，NO_DATA、SKIPPED、CANCELED 不折算为普通延误。

/control/phase?value=difference 返回只包含 vehicle 的差分包；value=deleted 删除 trip 实体；value=ambiguous 换静态表并给出缺服务日实体。供应商单次完整 feed 接收后才形成新当前状态。TripUpdate 与 VehiclePosition 关联同一班次但不共享更新时间字段。全部车辆与站点都是演练数据。

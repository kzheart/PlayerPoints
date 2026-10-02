# 木兰 PlayerPoints 队列修复

基于 Rosewood-Development/PlayerPoints 上游 096b12f（3.3.5），保留 GPL-3.0。木兰目标 Paper 1.21.1 / Java 21，版本 3.3.5-mulan.1；构建时使用该目标的 Paper API。源码未推送至上游。

木兰四货币压测发现 7 个专用账户的点券多出 10–30。原待写队列可在 getPendingTransactions 返回旧 deque 后，被异步线程复制并清空；生产线程随后追加到旧 deque 时，该交易可能遗漏。后台移走待写交易至更新缓存之间，有效余额也可能暂时遗漏正在写入的交易。

PointWriteQueue 将读取、校验、入队和批次交接置于同一短临界区；SQL 在临界区外运行，写线程串行提交不可变批次。已接受的余额在批次提交前保持可见，新交易不会追加到已交出的批次。提交后更新缓存并移除已清空的账户；在线刷新检查提交代次，旧读结果不能覆盖新交易。批次 SQL 与交易日志一起提交，失败保留异常状态并拒绝后续入队，不自动重放未知结果。BungeeCord 更新消息在提交后回到平台线程发送。

4 项测试覆盖：批次提交期间新入队不丢失；8 个生产线程与写线程并发尝试 2,000 次扣款，余额 1,000 时恰好 1,000 次接受；旧刷新不能覆盖新余额；存储失败拒绝后续写入。真实服验收见 ../MulanShop/docs/load-test-optimized-20261002.md。

此修复沿用现有 SQLite/MySQL 后端，不新增 PlayerPoints PostgreSQL 后端。它不提供跨节点账户行锁或商城多货币的统一事务；木兰 PG 共享金币由优化版 XConomy 提供，全服商城限购由 MulanShop PG 提供。管理员导入/全体批量改款、强杀恢复与共享 MySQL 的跨节点钱包原子性不在本次验证范围内。对账失败的旧压测证据保留，没有将该阶段标为通过。

构建：`JAVA_HOME=/path/to/jdk21 bash ./gradlew build`，产物 `build/libs/PlayerPoints-3.3.5-mulan.1.jar`。

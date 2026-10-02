# 木兰 PlayerPoints PostgreSQL 版本

Fork：<https://github.com/kzheart/PlayerPoints/tree/feat/mulan-postgresql>，上游 Rosewood-Development/PlayerPoints 096b12f（3.3.5），许可证 GPL-3.0，保留 LICENSE 并打入 JAR。编译 API Paper 1.21.1 / Java 21；木兰实际运行 Paper 1.21.11 build 132 / Java 25，版本 **3.3.5-mulan.3**。包含上一版修复待写交易队列丢失与批次提交期间有效余额错误的补丁。

## PostgreSQL 配置

插件 `config.yml` 增加以下配置。设置后重启；PG 优先于 mysql-settings，未启用 PG 时沿用 SQLite/MySQL 与队列修复。不同子服指向同一数据库、同一账户表才能共享余额。

```yaml
postgresql-settings:
  enabled: true
  hostname: 127.0.0.1
  port: 5432
  database-name: playerpoints
  user-name: playerpoints
  user-password: ''  # 由管理员填写，禁止提交真实凭据
  sslmode: verify-full
  connection-pool-size: 3  # 2-32，监听另占一个独立连接
  reconcile-interval-seconds: 30  # 5-3600，修改后重启
```

远端 PG 推荐 `verify-full` 与有效证书；木兰同机回环连接使用 `disable`。新库自动创建 `playerpoints_points`、`playerpoints_username_cache`、`playerpoints_transaction_log`、`playerpoints_pg_schema`。账户余额为非负 INTEGER（上限 2,147,483,647），revision 防止旧刷新覆盖新余额；初始化使用事务级 advisory lock，schema 不支持降级。

`storage/PgPointsRepository` 负责开户、行锁、余额与流水事务。`storage/PostgresConnector` 管理 Hikari 连接池，异常向上返回。`storage/PgPointsWallet` 分开管理交易队列（10,000）与读取队列（256），同账户并发显式读取合并。交易继续按本节点提交顺序串行执行；跨节点写入靠行锁协调。`manager/DataManager` 选择后端、预登录加载与加入/离线事件；`manager/PgPointsSyncManager` 管理跨服失效通知、100 ms 合并窗口和默认 30 秒在线账户批量校准。每批最多 256 个 UUID，一条只读 SELECT；不逐个查询，不在显示刷新中建账户。监听使用独立物理连接，不占用交易池。只在 PG 扣款事务中判断是否够钱，不相信本地显示缓存。零余额拒绝扣款，不用钳制负数掩盖超卖；溢出拒绝且事务回滚。

转账按 UUID 顺序锁两账户，两份余额、revision 与两条流水一起提交。事件仍按上游语义允许插件调整发送和接收量，因此其他插件显式修改事件时不承诺转账总量相等。没有事件改写时守恒。商城专用扣费接口拒绝改变金额的事件，保证按报价扣费和按原金额退款。

## API 与线程

原 `give/take/set/reset/pay/look` 等同步 API 保留。PG 写操作等提交后返回，旧调用者可能阻塞当前线程，不能据此声称所有经济插件都已异步。`look` 用显示缓存，预登录加载、本地提交直接更新、跨服提交通知后合并读取；不再每秒逐个轮询。通知断线、失败或丢失不会改变扣款判断，重连先 LISTEN 再校准在线账户，并有默认 30 秒兜底。离线账户须用 `lookAsync` 获得已提交余额；旧同步 `look` 的冷缓存仍可能等待读取。

新增 PG API：

- `postgresWalletEnabled()`：是否启用 PG 钱包。
- `changePointsAsync(UUID, int amount, boolean add)`：参数 amount 非负；在主线程调用，事件在主线程触发，JDBC 在专用线程执行，Future 仅在提交后返回成功。事件取消或金额改写返回 false。
- `payAsync(UUID source, UUID target, int amount)`：主线程触发双方事件，PG 事务一起完成；amount 必须正数，禁止给自己转账。
- `lookAsync(UUID)`：异步读取 PG 已提交余额。

MulanShop 已对接 `changePointsAsync`。金币与点券顺序提交，其他货币或玩家离线时按逆序等待退款完成。它们与奖励命令仍不是一个跨插件数据库事务：未知提交结果、退款失败进入商城人工核查，禁止盲重试。强杀、断电和网络分区自动恢复未验收。

PG 后端禁止在营业期间执行旧批量导入、legacy-table 导入、账户删除入口，避免绕过 revision 或覆盖跨服交易；逐账户 `giveAll`、普通 set/reset、全库 `offsetAllPoints` 可用。全库加减使用按 UUID 排序的行锁，与钱包扣款协调；全部余额和流水一起提交，任意账户溢出整体回滚，负数仍按上游 bulk 语义钳至 0。它沿用同步管理入口，可能等待大量账户处理。批量迁移和删除必须停服维护。SQLite/MySQL 原入口沿用既有行为。

## 迁移与测试

先停止所有子服并备份各自 SQLite。比对同 UUID 余额，冲突不能自动相加；在确认来源后导入空 PG，保留原库与流水备份。两服切换 PG 后逐账户核对，再开放业务。不要切回冻结的旧 SQLite，否则新 PG 交易丢失。

构建：`JAVA_HOME=/path/to/jdk21 bash ./gradlew build`，产物 `build/libs/PlayerPoints-3.3.5-mulan.3.jar`。

新增 8 项 PG 同步实库测试覆盖通知仅在提交后发布、回滚无通知、迁移保留余额、外部 SQL 递增版本、批量读取不建账户、双钱包通知同步、监听中断/重连、千账户四批读取、慢显示查询与扣款隔离/旧快照防倒退，以及关闭后请求不悬挂。

4 项队列测试覆盖提交期间入队、8 线程扣款、旧刷新与写失败。10 项 PG 实库测试覆盖同时建表、并发首次开户、8 连接抢余额、不同 Repository 并发偏移、相反转账、溢出与不足拒绝、余额/流水失败回滚、转账双账户回滚、批量钳零与批量溢出全回滚。普通本机构建未配置以下变量会明确跳过 PG 用例：

```sh
export PLAYERPOINTS_TEST_PG_URL='jdbc:postgresql://127.0.0.1:5432/playerpoints_test'
export PLAYERPOINTS_TEST_PG_USER='playerpoints_test'
export PLAYERPOINTS_TEST_PG_PASSWORD='...'
bash ./gradlew test --rerun-tasks
```

只使用隔离测试库，测试创建和删除专用前缀表。木兰隔离 PG 库已运行 18 项实库测试及 4 项队列测试，无跳过。历史两个 Minecraft 子服的并发与商城验收见 [PG 验收报告](docs/pg-validation-20261002.md)。新版同步设计见 [同步重设计](docs/pg-sync-redesign-20261002.md)。 两版实际 JAR 的 12 轮 PG 钱包 A/B 数据见 [性能对比](docs/pg-sync-performance-20261002.md)。

## 同步诊断与数据库迁移

`/points syncstatus`（权限 `playerpoints.syncstatus`，默认管理员）显示监听连接状态、订阅次数、收到的通知数、单账户/批量读任务次数及读写队列积压；命令本身没有 SQL，也不输出凭据。空闲时仍有 LISTEN 网络等待与约每 30 秒一次 `SELECT 1` 心跳，这不读取玩家余额。

schema 2 增加两个事务内触发器：更新强制递增 revision，INSERT/UPDATE 发送账户 UUID 与 revision 失效通知。通知不携带余额，收到通知后只能通过 PG 只读查询更新缓存；回滚不会发布通知。批量与第三方 SQL 更新也受触发器保护。SQL 直接删除账户仍属于停服维护，不支持在线绕过仓库删除账户再重新开户。

通知属于加速显示的提示，不是持久交易队列；断线期间可能遗漏。在线缓存默认 30 秒批量校准，数据库故障时会延后，不能宣称永远在 30 秒内刷新。通知队列耗尽时 PG 可能拒绝提交，按未知/失败结果处理，禁止盲重试。通知数据对同库其他角色可见，只发送 UUID 与 revision，不发送余额或密钥。

从 schema 1 自动事务迁移到 2，旧 mulan.2 不能在 schema 2 上重新初始化。回滚需停止所有子服，在保留当前余额和流水的前提下移除 points_revision/points_notify 触发器并把 schema 版本恢复为 1，再安装旧 JAR；不得直接恢复过期余额快照覆盖新交易。

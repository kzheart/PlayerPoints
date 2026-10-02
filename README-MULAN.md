# 木兰 PlayerPoints PostgreSQL 版本

Fork：<https://github.com/kzheart/PlayerPoints/tree/feat/mulan-postgresql>，上游 Rosewood-Development/PlayerPoints 096b12f（3.3.5），许可证 GPL-3.0，保留 LICENSE 并打入 JAR。目标 Paper 1.21.1 / Java 21，版本 **3.3.5-mulan.2**。包含上一版修复待写交易队列丢失与批次提交期间有效余额错误的补丁。

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
  connection-pool-size: 3
```

远端 PG 推荐 `verify-full` 与有效证书；木兰同机回环连接使用 `disable`。新库自动创建 `playerpoints_points`、`playerpoints_username_cache`、`playerpoints_transaction_log`、`playerpoints_pg_schema`。账户余额为非负 INTEGER（上限 2,147,483,647），revision 防止旧刷新覆盖新余额；初始化使用事务级 advisory lock，schema 不支持降级。

`storage/PgPointsRepository` 负责开户、行锁、余额与流水事务。`storage/PostgresConnector` 管理 Hikari 连接池，异常向上返回。`storage/PgPointsWallet` 管理有界 10,000 项工作队列与有限关闭等待。`manager/DataManager` 选择后端、预登录加载与每秒在线余额刷新。只在 PG 扣款事务中判断是否够钱，不相信本地显示缓存。零余额拒绝扣款，不用钳制负数掩盖超卖；溢出拒绝且事务回滚。

转账按 UUID 顺序锁两账户，两份余额、revision 与两条流水一起提交。事件仍按上游语义允许插件调整发送和接收量，因此其他插件显式修改事件时不承诺转账总量相等。没有事件改写时守恒。商城专用扣费接口拒绝改变金额的事件，保证按报价扣费和按原金额退款。

## API 与线程

原 `give/take/set/reset/pay/look` 等同步 API 保留。PG 写操作等提交后返回，旧调用者可能阻塞当前线程，不能据此声称所有经济插件都已异步。`look` 用显示缓存，预登录加载、每秒在线刷新；离线账户须用 `lookAsync` 获得已提交余额。

新增 PG API：

- `postgresWalletEnabled()`：是否启用 PG 钱包。
- `changePointsAsync(UUID, int amount, boolean add)`：参数 amount 非负；在主线程调用，事件在主线程触发，JDBC 在专用线程执行，Future 仅在提交后返回成功。事件取消或金额改写返回 false。
- `payAsync(UUID source, UUID target, int amount)`：主线程触发双方事件，PG 事务一起完成；amount 必须正数，禁止给自己转账。
- `lookAsync(UUID)`：异步读取 PG 已提交余额。

MulanShop 已对接 `changePointsAsync`。金币与点券顺序提交，其他货币或玩家离线时按逆序等待退款完成。它们与奖励命令仍不是一个跨插件数据库事务：未知提交结果、退款失败进入商城人工核查，禁止盲重试。强杀、断电和网络分区自动恢复未验收。

PG 后端禁止在营业期间执行旧批量导入、legacy-table 导入、账户删除入口，避免绕过 revision 或覆盖跨服交易；逐账户 `giveAll`、普通 set/reset、全库 `offsetAllPoints` 可用。全库加减使用按 UUID 排序的行锁，与钱包扣款协调；全部余额和流水一起提交，任意账户溢出整体回滚，负数仍按上游 bulk 语义钳至 0。它沿用同步管理入口，可能等待大量账户处理。批量迁移和删除必须停服维护。SQLite/MySQL 原入口沿用既有行为。

## 迁移与测试

先停止所有子服并备份各自 SQLite。比对同 UUID 余额，冲突不能自动相加；在确认来源后导入空 PG，保留原库与流水备份。两服切换 PG 后逐账户核对，再开放业务。不要切回冻结的旧 SQLite，否则新 PG 交易丢失。

构建：`JAVA_HOME=/path/to/jdk21 bash ./gradlew build`，产物 `build/libs/PlayerPoints-3.3.5-mulan.2.jar`。

4 项队列测试覆盖提交期间入队、8 线程扣款、旧刷新与写失败。10 项 PG 实库测试覆盖同时建表、并发首次开户、8 连接抢余额、不同 Repository 并发偏移、相反转账、溢出与不足拒绝、余额/流水失败回滚、转账双账户回滚、批量钳零与批量溢出全回滚。普通本机构建未配置以下变量会明确跳过 PG 用例：

```sh
export PLAYERPOINTS_TEST_PG_URL='jdbc:postgresql://127.0.0.1:5432/playerpoints_test'
export PLAYERPOINTS_TEST_PG_USER='playerpoints_test'
export PLAYERPOINTS_TEST_PG_PASSWORD='...'
./gradlew test --rerun-tasks
```

只使用隔离测试库，测试创建和删除专用前缀表。木兰已用真实 PG 运行 10 项用例，无跳过。实际两个 Minecraft 子服的并发与商城验收见 [PG 验收报告](docs/pg-validation-20261002.md)。

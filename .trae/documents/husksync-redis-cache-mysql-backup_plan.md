# HuskSync 改造计划：Redis 仅缓存 + MySQL 定时备份

## 概述

将 HuskSync 插件从当前的 **MySQL/Redis 双存储架构** 改造为：
1. **Redis 作为玩家背包数据的唯一缓存层**（主要读写走 Redis，MySQL 仅用于持久化备份）
2. **新增 MySQL 数据库定时自动备份功能**，备份间隔可在配置文件中调整

---

## 当前架构分析

### 现有数据流
```
玩家加入 → 锁定 → 从 Redis 获取最新数据(有则用,无则从MySQL加载) → 应用到玩家
玩家退出 → 锁定 → 序列化数据 → 保存到 MySQL (addSnapshot) → 写入 Redis 缓存 → 解锁
```

### 关键文件
| 文件 | 作用 |
|------|------|
| [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java) | 配置类（含 DatabaseSettings、RedisSettings、SynchronizationSettings） |
| [RedisManager.java](common/src/main/java/net/william278/husksync/redis/RedisManager.java) | Redis 连接管理、Pub/Sub、数据缓存（setUserData/getUserData） |
| [Database.java](common/src/main/java/net/william278/husksync/database/Database.java) | 数据库抽象类（CRUD、快照轮转） |
| [MySqlDatabase.java](common/src/main/java/net/william278/husksync/database/MySqlDatabase.java) | MySQL/MariaDB 实现（HikariCP 连接池） |
| [DataSyncer.java](common/src/main/java/net/william278/husksync/sync/DataSyncer.java) | 数据同步器基类（saveData/addSnapshotToDatabase） |
| [LockstepDataSyncer.java](common/src/main/java/net/william278/husksync/sync/LockstepDataSyncer.java) | 锁步模式同步器 |
| [DelayDataSyncer.java](common/src/main/java/net/william278/husksync/sync/DelayDataSyncer.java) | 延迟模式同步器 |
| [BukkitHuskSync.java](bukkit/src/main/java/net/william278/husksync/BukkitHuskSync.java) | Bukkit 主入口（初始化流程） |
| [RedisKeyType.java](common/src/main/java/net/william278/husksync/redis/RedisKeyType.java) | Redis Key 类型定义 |

### 当前 Redis 使用方式
- `LATEST_SNAPSHOT`: 缓存玩家最新数据快照（TTL 1年），`setUserData()` 写入，`getUserData()` 读取并**消费删除**
- `DATA_CHECKOUT`: Lockstep 模式的数据检出锁
- `SERVER_SWITCH`: Delay 模式的服务器切换标记
- Pub/Sub 通道: `UPDATE_USER_DATA`, `REQUEST_USER_DATA`, `RETURN_USER_DATA`, `CHECK_IN_PETITION`

---

## 改动方案

### 第一步：修改配置文件 - 新增备份设置

**文件**: [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java)

在 `SynchronizationSettings` 类中新增 `BackupSettings` 内部类：

```java
// 新增配置项
@Comment("MySQL数据库自动备份设置")
private BackupSettings backup = new BackupSettings();

@Getter
@Configuration
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public static class BackupSettings {
    @Comment("是否启用MySQL数据库自动备份")
    private boolean enabled = true;

    @Comment("备份间隔时间（分钟），默认每6小时备份一次")
    private long intervalMinutes = 360;

    @Comment("备份数据保存目录（相对于插件数据文件夹）")
    private String backupDirectory = "backups";

    @Comment("最大保留备份数量，0表示不限制")
    private int maxBackups = 30;
}
```

### 第二步：创建 MySQL 备份管理器

**新文件**: `common/src/main/java/net/william278/husksync/database/DatabaseBackupManager.java`

功能：
- 使用 `mysqldump` 命令行工具通过 `ProcessBuilder` 执行数据库备份
- 支持定时任务调度（使用现有的 `Task.Repeating` 机制）
- 备份文件命名格式：`husksync_backup_yyyy-MM-dd_HH-mm-ss.sql`
- 自动清理过期备份（按 maxBackups 限制）
- 备份前先执行 `FLUSH TABLES WITH READ LOCK` 保证一致性（可选）
- 记录备份日志

核心方法：
- `initialize()` - 注册定时备份任务
- `executeBackup()` - 执行一次备份操作
- `cleanupOldBackups()` - 清理超出数量限制的旧备份
- `terminate()` - 停止备份任务

### 第三步：修改 DataSyncer - 将 MySQL 写入改为异步批量持久化

**文件**: [DataSyncer.java](common/src/main/java/net/william278/husksync/sync/DataSyncer.java)

修改 `addSnapshotToDatabase()` 方法：
- 玩家数据**优先且仅写入 Redis**（作为主缓存）
- MySQL 的 `addSnapshot()` 调用改为**可选的异步后台持久化**
- 新增标志位控制是否同时写 MySQL（默认开启作为备份）

```java
// 修改后的逻辑：
// 1. 先写 Redis（主缓存，立即生效）
// 2. 异步写 MySQL（持久化备份，可配置是否执行）
private void addSnapshotToDatabase(@NotNull User user, @NotNull DataSnapshot.Packed data,
                                   @Nullable BiConsumer<User, DataSnapshot.Packed> after) {
    // Redis 作为主缓存 - 始终写入
    getRedis().setUserData(user, data);

    // MySQL 作为持久化备份 - 异步写入
    if (plugin.getSettings().getSynchronization().isPersistToMysql()) {
        plugin.runAsync(() -> getDatabase().addSnapshot(user, data));
    }

    if (after != null) {
        after.accept(user, data);
    }
}
```

### 第四步：修改 LockstepDataSyncer - 适配 Redis 优先读取

**文件**: [LockstepDataSyncer.java](common/src/main/java/net/william278/husksync/sync/LockstepDataSyncer.java)

修改 `syncApplyUserData()` 方法：
- **优先从 Redis 读取**玩家数据（不再先 checkout 再读）
- Redis 无数据时再 fallback 到 MySQL 加载
- 读取后不删除 Redis key（改为非消费模式，因为 Redis 是主存储）

需要新增一个 `getUserDataNoConsume()` 方法到 RedisManager。

### 第五步：修改 DelayDataSyncer - 适配 Redis 优先读取

**文件**: [DelayDataSyncer.java](common/src/main/java/net/william278/husksync/sync/DelayDataSyncer.java)

同样修改为 Redis 优先读取策略。

### 第六步：修改 RedisManager - 新增非消费式读取方法

**文件**: [RedisManager.java](common/src/main/java/net/william278/husksync/redis/RedisManager.java)

新增方法：
- `getUserDataNoConsume(User)` - 读取数据但**不删除** key（因为 Redis 现在是主缓存而非临时中转站）

修改现有方法：
- `getUserData()` 保持不变（仍用于跨服务器数据传输时的消费式读取）
- `setUserData()` TTL 可调整为更长或永久（因为现在是主存储）

### 第七步：修改 BukkitHuskSync - 初始化备份管理器

**文件**: [BukkitHuskSync.java](bukkit/src/main/java/net/william278/husksync/BukkitHuskSync.java)

在 `onEnable()` 中，初始化数据库之后、Redis 初始化之前：
- 创建并初始化 `DatabaseBackupManager`
- 在 `onDisable()` 中终止备份管理器

### 第八步：修改 Settings - 新增 persistToMysql 开关

**文件**: [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java)

在 `SynchronizationSettings` 中新增：
```java
@Comment("是否将玩家数据持久化到MySQL数据库（作为Redis缓存的备份）。关闭则仅使用Redis存储")
private boolean persistToMysql = true;
```

---

## 文件改动清单

| 文件 | 操作 | 改动说明 |
|------|------|----------|
| `common/.../config/Settings.java` | 修改 | 新增 BackupSettings 配置组 + persistToMysql 开关 |
| `common/.../redis/RedisManager.java` | 修改 | 新增 getUserDataNoConsume() 非消费式读取 |
| `common/.../sync/DataSyncer.java` | 修改 | 改造 saveData 流程：Redis 优先 + MySQL 异步持久化 |
| `common/.../sync/LockstepDataSyncer.java` | 修改 | 适配 Redis 优先读取策略 |
| `common/.../sync/DelayDataSyncer.java` | 修改 | 适配 Redis 优先读取策略 |
| `common/.../database/DatabaseBackupManager.java` | **新建** | MySQL 定时备份管理器 |
| `bukkit/.../BukkitHuskSync.java` | 修改 | 初始化备份管理器 |

---

## 风险与注意事项

1. **Redis 内存占用**：将 Redis 作为主存储意味着所有在线玩家数据常驻内存。需确保 Redis 有足够的内存配置。
2. **mysqldump 依赖**：备份功能依赖系统安装了 `mysqldump` 命令行工具，需要在初始化时检测其可用性。
3. **数据一致性**：Redis 作为主缓存时，如果 Redis 宕机而 persistToMysql 关闭，数据可能丢失。建议保持 persistToMysql 默认开启。
4. **跨服务器兼容性**：改造需保证多服务器环境下数据同步仍然正常工作（Pub/Sub 机制不变）。

---

## 验证步骤

1. 编译项目确认无语法错误
2. 启动服务器检查日志确认备份管理器正确初始化
3. 测试玩家加入/退出，验证数据正确写入 Redis
4. 检查 MySQL 是否按预期收到持久化数据
5. 等待一个备份周期（或手动触发），验证备份 SQL 文件生成正确
6. 验证旧备份清理功能正常工作

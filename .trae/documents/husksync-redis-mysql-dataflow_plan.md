# HuskSync 数据流改造计划：Redis 实时缓存 + MySQL 离线持久化

## 概述

用户明确了具体的数据流要求：
1. **Redis 和 MySQL 均为必需**（非可选）
2. **玩家物品变更时** → 实时同步到 Redis（不走 MySQL）
3. **玩家离线时** → 数据保存到 MySQL（持久化）
4. **玩家下次上线时** → 从 MySQL 加载数据到 Redis，再从 Redis 应用到玩家

---

## 当前代码状态分析

### 现有数据流（上一轮改造后的状态）

```
任何保存操作(saveData):
  ├── 同步写 Redis (setUserData)
  └── 异步写 MySQL (addSnapshot) ← 受 persistToMysql 控制，默认开启

玩家加入(syncApplyUserData):
  └── Redis no-consume读取 → Redis consume读取 → MySQL fallback (三级)

玩家退出(syncSaveUserData):
  └── saveData(DISCONNECT) → Redis + MySQL 同时写
```

### 问题

当前实现中 **所有保存操作都会同时写 Redis 和 MySQL**，不符合用户"物品变更只写 Redis、离线才写 MySQL"的需求。

### 需要修改的核心触发点

| 触发场景 | 调用方法 | 当前行为 | 期望行为 |
|----------|----------|----------|----------|
| 玩家加入 | `syncApplyUserData` | Redis优先读→MySQL兜底 | **MySQL加载→写入Redis→应用** |
| 玩家退出 | `syncSaveUserData` | Redis+MySQL同时写 | **Redis+MySQL都写** |
| 物品变更(世界保存等) | `saveCurrentUserData` | Redis+MySQL同时写 | **仅写Redis** |
| 服务端关闭 | `saveCurrentUserData(SHUTDOWN)` | Redis+MySQL同时写 | **Redis+MySQL都写** |
| 玩家死亡 | `saveData(DEATH)` | Redis+MySQL同时写 | **MySQL持久化(+Redis可选)** |

---

## 改动方案

### 第一步：重构 DataSyncer - 分离 Redis-only 和 MySQL 写入路径

**文件**: [DataSyncer.java](common/src/main/java/net/william278/husksync/sync/DataSyncer.java)

将 `addSnapshotToDatabase()` 拆分为两个清晰的方法：

```java
// === 新增方法 ===

/** 仅将数据同步到 Redis（用于实时物品变更） */
@Blocking
private void saveToRedis(@NotNull User user, @NotNull DataSnapshot.Packed data) {
    getRedis().setUserData(user, data);
}

/** 将数据持久化到 MySQL 并同步到 Redis（用于离线/关机场景） */
@Blocking
private void saveToMysqlAndRedis(@NotNull User user, @NotNull DataSnapshot.Packed data,
                                   @Nullable BiConsumer<User, DataSnapshot.Packed> after) {
    // 1. 同步写 Redis（保证跨服务器切换能拿到最新数据）
    getRedis().setUserData(user, data);
    // 2. 同步写 MySQL（持久化，必需）
    getDatabase().addSnapshot(user, data);
    if (after != null) {
        after.accept(user, data);
    }
}
```

修改现有方法：

```java
// saveCurrentUserData - 物品变更等实时操作：仅写 Redis
public void saveCurrentUserData(@NotNull OnlineUser onlineUser, @NotNull DataSnapshot.SaveCause cause) {
    final DataSnapshot.Packed snapshot = onlineUser.createSnapshot(cause);
    if (!snapshot.getSaveCause().fireDataSaveEvent()) {
        saveToRedis(onlineUser, snapshot);
        return;
    }
    plugin.fireEvent(
        plugin.getDataSaveEvent(onlineUser, snapshot),
        (event) -> saveToRedis(onlineUser, snapshot)
    );
}

// 新增：离线保存方法（Redis + MySQL）
public void saveUserDataOnDisconnect(@NotNull OnlineUser onlineUser, @NotNull DataSnapshot.SaveCause cause) {
    saveData(onlineUser, onlineUser.createSnapshot(cause),
             (user, data) -> getRedis().setUserData(user, data));
}
// 注意：saveData 内部调用 addSnapshotToDatabase → 改为 saveToMysqlAndRedis
```

关键改动：`addSnapshotToDatabase()` 改名为逻辑更清晰的实现：

```java
// 原 addSnapshotToDatabase 替换为 saveToMysqlAndRedis 逻辑
// 即：始终同步写 Redis + MySQL（不再有 persistToMysql 开关控制）
@Blocking
private void addSnapshotToDatabase(@NotNull User user, @NotNull DataSnapshot.Packed data,
                                   @Nullable BiConsumer<User, DataSnapshot.Packed> after) {
    getRedis().setUserData(user, data);
    getDatabase().addSnapshot(user, data);  // 同步写入，不再是异步
    if (after != null) {
        after.accept(user, data);
    }
}
```

新增 MySQL→Redis 加载方法：

```java
/** 从 MySQL 加载数据并写入 Redis */
@Blocking
protected void loadFromMysqlToRedis(@NotNull OnlineUser user) {
    getDatabase().getLatestSnapshot(user).ifPresent(snapshot -> {
        getRedis().setUserData(user, snapshot);
        plugin.debug("[%s] Loaded data from MySQL into Redis".formatted(user.getName()));
    });
}
```

### 第二步：修改 LockstepDataSyncer - 适配新数据流

**文件**: [LockstepDataSyncer.java](common/src/main/java/net/william278/husksync/sync/LockstepDataSyncer.java)

```java
// 玩家加入：从 MySQL 加载 → 写入 Redis → 从 Redis 应用
@Override
public void syncApplyUserData(@NotNull OnlineUser user) {
    this.listenForRedisData(user, () -> {
        if (user.cannotApplySnapshot()) return false;

        // Checkout 检查保持不变
        final Optional<String> server = getRedis().getUserCheckedOut(user);
        if (server.isPresent() && !server.get().equals(plugin.getServerName())) {
            if (plugin.getSettings().getSynchronization().isCheckinPetitions()) {
                getRedis().petitionServerCheckin(server.get(), user);
            }
            return false;
        }

        getRedis().setUserCheckedOut(user, true);

        // ★ 核心改动：先从 MySQL 加载到 Redis，再从 Redis 应用
        loadFromMysqlToRedis(user);           // MySQL → Redis
        getRedis().getUserDataNoConsume(user)   // Redis → Player
            .ifPresentOrElse(
                data -> user.applySnapshot(data, DataSnapshot.UpdateCause.SYNCHRONIZED),
                () -> user.completeSync(true, DataSnapshot.UpdateCause.NEW_USER, plugin)
            );
        return true;
    });
}

// 玩家离线：保存到 MySQL + Redis
@Override
public void syncSaveUserData(@NotNull OnlineUser onlineUser) {
    plugin.runAsync(() -> saveData(
        onlineUser,
        onlineUser.createSnapshot(DataSnapshot.SaveCause.DISCONNECT),
        (user, data) -> {
            getRedis().setUserCheckedOut(user, false);
            plugin.unlockPlayer(user.getUuid());
        }
    ));
    // saveData → addSnapshotToDatabase → Redis + MySQL 都写
}
```

### 第三步：修改 DelayDataSyncer - 适配新数据流

**文件**: [DelayDataSyncer.java](common/src/main/java/net/william278/husksync/sync/DelayDataSyncer.java)

```java
@Override
public void syncApplyUserData(@NotNull OnlineUser user) {
    plugin.runAsyncDelayed(() -> {
        if (!getRedis().getUserServerSwitch(user)) {
            // 非跨服切换：MySQL → Redis → Player
            loadFromMysqlToRedis(user);
            getRedis().getUserDataNoConsume(user).ifPresentOrElse(
                data -> user.applySnapshot(data, DataSnapshot.UpdateCause.SYNCHRONIZED),
                () -> user.completeSync(true, DataSnapshot.UpdateCause.NEW_USER, plugin)
            );
            return;
        }
        // 跨服切换：监听 Redis 消费式数据
        this.listenForRedisData(user, () -> getRedis().getUserData(user)
            .map(data -> { user.applySnapshot(data, ...); return true; }).orElse(false));
    }, ...);
}

@Override
public void syncSaveUserData(@NotNull OnlineUser onlineUser) {
    plugin.runAsync(() -> {
        getRedis().setUserServerSwitch(onlineUser);
        saveData(onlineUser, onlineUser.createSnapshot(DataSnapshot.SaveCause.DISCONNECT),
            (user, data) -> plugin.unlockPlayer(user.getUuid()));
        // saveData → addSnapshotToDatabase → Redis + MySQL 都写
    });
}
```

### 第四步：修改 Settings - 移除 persistToMysql 或调整语义

**文件**: [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java)

由于 Redis 和 MySQL 都是必需的，移除 `persistToMysql` 配置项（或保留但标注为"预留选项，当前强制开启"）。

建议直接移除该字段和 getter，简化配置。

### 第五步：修改 EventListener - 关闭时使用完整保存

**文件**: [EventListener.java](common/src/main/java/net/william278/husksync/listener/EventListener.java)

`handlePluginDisable()` 中调用的 `saveCurrentUserData(SERVER_SHUTDOWN)` 应改为走 **Redis+MySQL 路径**（因为服务端关闭等同于所有玩家离线）。

需要新增一个方法或修改判断逻辑：SERVER_SHUTDOWN 时也写入 MySQL。

方案：在 DataSyncer 中新增 `saveCurrentUserDataForShutdown()` 方法，或者让 `saveCurrentUserData` 根据 SaveCause 判断是否写 MySQL。

推荐方案：在 `saveCurrentUserData` 中增加判断 —— SERVER_SHUTDOWN 和 DISCONNECT 走 MySQL+Redis，其他走 Redis-only。

---

## 文件改动清单

| 文件 | 操作 | 改动说明 |
|------|------|----------|
| [DataSyncer.java](common/src/main/java/net/william278/husksync/sync/DataSyncer.java) | **重写核心逻辑** | 拆分 saveToRedis/saveToMysqlAndRedis；修改 saveCurrentUserData 为 Redis-only；新增 loadFromMysqlToRedis；修改 addSnapshotToDatabase 为同步双写 |
| [LockstepDataSyncer.java](common/src/main/java/net/william278/husksync/sync/LockstepDataSyncer.java) | **修改** | 加入流程改为 MySQL→Redis→Player；退出保持双写 |
| [DelayDataSyncer.java](common/src/main/java/net/william278/husksync/sync/DelayDataSyncer.java) | **修改** | 同上适配 |
| [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java) | **小改** | 移除 persistToMysql 字段（两者均为必需） |
| [EventListener.java](common/src/main/java/net/william278/husksync/listener/EventListener.java) | **小改** | SHUTDOWN 场景走 MySQL 双写路径 |

> 注：[RedisManager.java](common/src/main/java/net/william278/husksync/redis/RedisManager.java)、[DatabaseBackupManager.java](common/src/main/java/net/william278/husksync/database/DatabaseBackupManager.java)、[BukkitHuskSync.java](bukkit/src/main/java/net/william278/husksync/BukkitHuskSync.java) 保持上一轮的改动不变。

---

## 最终数据流图

```
┌─────────────────────────────────────────────────────────────┐
│                     玩家在线期间                              │
│                                                              │
│  物品变更/世界保存 → saveCurrentUserData()                   │
│       ↓                                                      │
│    【仅写 Redis】setUserData()  ← 实时同步，低延迟            │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                     玩家离线时                                │
│                                                              │
│  玩家退出/服务器关闭 → syncSaveUserData() / handleDisable()  │
│       ↓                                                      │
│    【双写】① setUserData(Redis) + ② addSnapshot(MySQL)      │
│       ↓                                                      │
│    MySQL 作为持久化存储（下次上线的数据源）                    │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                     玩家再次上线                              │
│                                                              │
│  syncApplyUserData()                                         │
│       ↓                                                      │
│    ① getLatestSnapshot(MySQL) ← 从数据库加载                 │
│       ↓                                                      │
│    ② setUserData(Redis)     ← 写入 Redis 缓存               │
│       ↓                                                      │
│    ③ getUserDataNoConsume(Redis) ← 从 Redis 读取应用         │
│       ↓                                                      │
│    ④ applySnapshot(Player)   ← 应用到玩家                    │
│                                                              │
│  之后所有物品变更 → 回到【仅写 Redis】循环                     │
└─────────────────────────────────────────────────────────────┘
```

---

## 验证步骤

1. 编译通过无错误
2. 启动服务器，确认备份管理器正常初始化
3. 测试玩家加入：观察日志确认 "Loaded data from MySQL into Redis"
4. 修改玩家背包物品：确认 Redis 有更新（不触发 MySQL 写入）
5. 玩家退出：确认 MySQL 有新快照记录
6. 玩家重新加入：确认从 MySQL 加载了上次退出的数据
7. 备份定时任务正常执行

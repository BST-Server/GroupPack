# Redis 完全接管背包数据方案（拦截+替换）

## 概述

用户选择**完全接管模式**：游戏不再作为背包数据的权威来源，Redis 是唯一管理者。
核心机制：**拦截所有背包操作 → 同步到 Redis → 从 Redis 反向强制覆盖回玩家**，形成闭环控制。

---

## 当前状态分析

### 已实现（上一轮）
- [InventorySyncListener.java](bukkit/src/main/java/net/william278/husksync/listener/InventorySyncListener.java) 已创建
  - 拦截 `InventoryClickEvent` / `InventoryDragEvent` / `EntityPickupItemEvent` / `PlayerDropItemEvent`
  - 通过防抖队列将变更**单向同步到 Redis**
  - **缺失**：没有从 Redis 反向覆盖回玩家的机制

### 缺失的关键机制

当前实现是**单向的**（Player → Redis），要实现"游戏不管理背包"，需要增加**反向强制**（Redis → Player）：

```
当前（单向）:   玩家操作 → 拦截事件 → 写入 Redis     ✅ 已有
完全接管（双向）: 玩家操作 → 拦截事件 → 写入 Redis → 强制从Redis读回覆盖玩家  ❌ 缺失
                                    ↑                                      ↓
                              其他服修改Redis ←←←← 跨服推送/API/管理员命令 ←┘
```

---

## 改动方案

### 核心设计：双向闭环

```
┌──────────────────────────────────────────────────────┐
│                  完全接管循环                          │
│                                                      │
│  ① 玩家执行背包操作（点击/拖拽/拾取/丢弃）             │
│       ↓                                              │
│  ② InventorySyncListener 拦截事件                     │
│       ↓                                              │
│  ③ createSnapshot() → setUserData(Redis)  【写入】    │
│       ↓                                              │
│  ④ 延迟 1 tick 后                                   │
│  ⑤ getUserDataNoConsume(Redis) → applySnapshot(Player)【读取+覆盖】│
│       ↓                                              │
│  ⑥ 玩家看到的背包 = Redis 中的权威数据                │
│                                                      │
│  ⑦ 定时兜底（每 N 秒）：对所有在线玩家执行 ④→⑤        │
│      防止任何绕过事件系统的背包修改                    │
└──────────────────────────────────────────────────────┘
```

### 第一步：重写 InventorySyncListener - 增加反向覆盖

**文件**: [InventorySyncListener.java](bukkit/src/main/java/net/william278/husksync/listener/InventorySyncListener.java)

改造核心逻辑：

```java
@EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
public void onInventoryClick(InventoryClickEvent event) {
    if (!(event.getWhoClicked() instanceof Player player)) return;
    if (shouldIgnorePlayer(player)) return;
    final OnlineUser user = BukkitUser.adapt(player, plugin);

    // Step 1: Sync current state to Redis (before the click takes effect,
    //         we capture what's about to change; after the click, we'll re-sync)
    // Actually: let the click happen first, then sync + override
    scheduleFullSync(user);
}

// 新的完整同步流程：写Redis → 延迟 → 读Redis → 覆盖玩家
private void scheduleFullSync(@NotNull OnlineUser user) {
    if (!enabled) return;

    // Phase 1 (immediate): Write current state to Redis
    pendingWrites.put(user.getUuid(), System.currentTimeMillis());
    // Phase 2 (delayed, next tick): Read from Redis and force-apply to player
    pendingOverrides.put(user.getUuid(), System.currentTimeMillis());
}
```

处理流程改为两阶段：

**Phase 1 - 写入 Redis**（防抖后立即执行）：
```java
private void processPendingWrites() {
    // 遍历 pendingWrites，超时的执行 createSnapshot → setUserData(Redis)
    // 与现有 processPendingSyncs() 逻辑相同
}
```

**Phase 2 - 从 Redis 反向覆盖**（写入完成后延迟 1 tick 执行）：
```java
private void processPendingOverrides() {
    // 遍历 pendingOverrides，超时的执行:
    //   getUserDataNoConsume(Redis) → applySnapshot(Player)
    // 这确保玩家背包始终等于 Redis 中的数据
}
```

使用**两个独立的防抖队列**和**两个定时任务**：
- `writeTask`：处理写入 Redis（间隔 = debounceTicks）
- `overrideTask`：处理从 Redis 覆盖回玩家（间隔 = debounceTicks + 1 tick，确保在写入之后）

### 第二步：新增定时强制校验 - 兜底机制

即使所有事件都被拦截，仍需一个周期性任务作为安全网：
- 每 `enforceInterval` 秒（可配置，默认 5 秒）
- 遍历所有在线玩家
- 对每个玩家：从 Redis 读取 → applySnapshot 到玩家
- 这能纠正任何可能绕过事件监听器的背包修改（其他插件、作弊等）

```java
// 在 initialize() 中启动
this.enforceTask = plugin.getRepeatingTask(this::enforceAllFromRedis, enforceIntervalTicks);

private void enforceAllFromRedis() {
    if (!enabled) return;
    plugin.getOnlineUsers().forEach(user -> {
        if (plugin.isLocked(user.getUuid())) return;
        plugin.runAsync(() -> {
            try {
                getRedis().getUserDataNoConsume(user).ifPresent(snapshot ->
                    user.applySnapshot(snapshot, DataSnapshot.UpdateCause.SYNCHRONIZED));
            } catch (Throwable e) { /* log */ }
        });
    });
}
```

### 第三步：更新 Settings 配置

**文件**: [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java)

扩展 `InventorySyncSettings`：

```java
public static class InventorySyncSettings {
    @Comment("Enable full Redis inventory takeover mode")
    private boolean enabled = true;

    @Comment("Debounce interval in ticks between syncs (default: 10 ticks = 500ms)")
    private long debounceTicks = 10;

    @Comment("Periodic enforcement interval in seconds (default: 5). "
            + "Forces all online players' inventories to match Redis. Set to 0 to disable.")
    private long enforceIntervalSeconds = 5;
}
```

### 第四步：线程安全注意事项

关键点：`applySnapshot()` 内部会切换到主线程 (`runSync`)。我们的定时任务运行在异步线程，需要正确处理：

```java
// processPendingOverrides 中:
plugin.runAsync(() -> {
    getRedis().getUserDataNoConsume(user).ifPresent(snapshot ->
        // applySnapshot 内部会自行切到主线程
        user.applySnapshot(snapshot, DataSnapshot.UpdateCause.SYNCHRONIZED)
    );
});
```

`UserDataHolder.applySnapshot()` 的实现已经处理了异步→主线程的切换（见 UserDataHolder.java 第 126 行 `plugin.runSync(...)`），所以调用方只需在任意线程调用即可。

### 第五步：防止无限循环

需要确保不会出现以下死循环：
1. applySnapshot 覆盖玩家背包 → 触发新的 InventoryEvent → 再次触发 listener → 再次 applySnapshot...

**解决方案**：在 `processPendingOverrides` 执行期间，临时标记该玩家为"正在被覆盖"，`shouldIgnorePlayer()` 返回 true：

```java
private final Set<UUID> overridingPlayers = ConcurrentHashMap.newKeySet();

private boolean shouldIgnorePlayer(@NotNull Player player) {
    return plugin.isLocked(player.getUniqueId())
        || overridingPlayers.contains(player.getUniqueId());  // 新增
}

// 在 processPendingOverrides 中:
overridingPlayers.add(user.getUuid());
try {
    user.applySnapshot(snapshot, ...);
} finally {
    overridingPlayers.remove(user.getUuid());
}
```

---

## 文件改动清单

| 文件 | 操作 | 改动说明 |
|------|------|----------|
| [InventorySyncListener.java](bukkit/src/main/java/net/william278/husksync/listener/InventorySyncListener.java) | **重写** | 双向闭环：写入Redis + 从Redis反向覆盖 + 定时强制校验 + 防循环保护 |
| [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java) | **小改** | InventorySyncSettings 新增 enforceIntervalSeconds |

> 其他文件（DataSyncer / LockstepDataSyncer / DelayDataSyncer / RedisManager / DatabaseBackupManager / BukkitHuskSync）保持不变。

---

## 最终数据流图

```
                    ┌─────────────────────────┐
                    │        Redis            │
                    │   (唯一权威数据源)       │
                    └──────┬──────────┬────────┘
                           │          │
              写入 ↑       │          │ ↓ 读取+覆盖
                           │          │
  ┌────────────────────────┼──────────┼────────────────────────┐
  │                        │          │                        │
  │  玩家操作              │          │  强制覆盖               │
  │  (点击/拖拽/拾取/丢弃)  │          │  (applySnapshot)        │
  │       ↓               │          │       ↑                │
  │  事件拦截             │          │  定时校验(每5秒)        │
  │       ↓               │          │  或事件驱动(防抖后)      │
  │  createSnapshot        │          │                        │
  │       ↓               │          │                        │
  │  setUserData(Redis) ───┼──────────┘                        │
  │                                                        │
  │  Minecraft 游戏客户端                                  │
  │  (仅作为显示层，不管理数据)                             │
  └────────────────────────────────────────────────────────┘

  离线时: saveUserDataOnDisconnect → Redis + MySQL 双写
  上线时: MySQL → Redis → Player (Redis 始终是中间权威层)
```

---

## 验证步骤

1. 编译通过无错误
2. 启动服务器，确认日志显示 "Real-time inventory sync listener initialized"
3. 进入游戏，移动物品 → 观察物品位置是否稳定（无抖动/闪烁）
4. 使用 Redis CLI 直接修改某玩家的背包数据 → 观察是否在 enforceInterval 内自动覆盖到玩家
5. 测试跨服务器切换：A服修改背包 → 切换到B服 → B服应显示A服的数据
6. 测试退出/重新加入：背包数据完整保留

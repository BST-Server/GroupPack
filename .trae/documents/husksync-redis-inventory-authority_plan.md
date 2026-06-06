# Redis 全权管理玩家背包数据 方案

## 概述

将玩家背包（Inventory）数据的**唯一管理权**交给 Redis，Minecraft 游戏本身不再作为背包数据的权威来源。
任何游戏内的背包变更操作都会**实时同步到 Redis**，Redis 始终保持最新权威状态。

---

## 当前架构分析

### 现有背包数据流

```
[游戏内操作] → 玩家点击/拖拽物品 → Minecraft 原生处理背包变化
                                          ↓
                    [仅在特定时机] 世界保存/退出/死亡 → createSnapshot() → Redis/MySQL
```

**问题**：游戏内的大量背包操作（拾取、丢弃、移动、合成等）**不会被实时捕获**到 Redis。只有在 `saveCurrentUserData` 被显式调用时（世界保存、退出等）才会同步。

### 需要拦截的 Bukkit 事件

| 事件类 | 触发时机 | 说明 |
|--------|----------|------|
| `InventoryClickEvent` | 玩家点击背包格子 | 最核心的背包操作 |
| `InventoryDragEvent` | 玩家拖拽物品 | 批量物品移动 |
| `InventoryMoveItemEvent` | 物品在容器间移动（漏斗等） | 服务端触发的移动 |
| `EntityPickupItemEvent` | 玩家拾取地面物品 | 获取新物品 |

---

## 改动方案

### 第一步：新建 InventorySyncListener - 实时背包同步监听器

**新文件**: `bukkit/src/main/java/net/william278/husksync/listener/InventorySyncListener.java`

功能：
- 注册为 Bukkit 事件监听器（HIGH 优先级）
- 监听所有背包变更事件（Click、Drag、Pickup、Drop 等）
- **事件发生后**：读取玩家当前背包状态 → 序列化 → 写入 Redis
- 使用**防抖(debounce)机制**避免高频写入（同一玩家在短时间内的多次操作合并为一次写入）
- 可配置是否启用（通过 Settings）

核心逻辑：

```java
public class InventorySyncListener implements Listener {
    private final HuskSync plugin;
    // 防抖：每个玩家的最后一次操作时间
    private final Map<UUID, Long> pendingSyncs = new ConcurrentHashMap<>();
    // 防抖间隔（tick），默认 10 tick (500ms)
    private static final long DEBOUNCE_TICKS = 10;
    // 定时任务：处理防抖队列
    private Task.Repeating syncTask;

    @EventHandler(priority = EventPriority.HIGH)
    public void onInventoryClick(InventoryClickEvent event) {
        if (!(event.getWhoClicked() instanceof Player player)) return;
        scheduleRedisSync(plugin.adapt(player));
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onInventoryDrag(InventoryDragEvent event) {
        if (!(event.getWhoClicked() instanceof Player player)) return;
        scheduleRedisSync(plugin.adapt(player));
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onEntityPickupItem(EntityPickupItemEvent event) {
        if (!(event.getEntity() instanceof Player player)) return;
        scheduleRedisSync(plugin.adapt(player));
    }

    // 防抖调度：记录时间戳，由定时任务统一执行实际写入
    private void scheduleRedisSync(@NotNull OnlineUser user) {
        pendingSyncs.put(user.getUuid(), System.currentTimeMillis());
    }

    // 定时任务：每 DEBOUNCE_TICKS 检查一次待同步队列
    private void processPendingSyncs() {
        final long now = System.currentTimeMillis();
        pendingSyncs.entrySet().removeIf(entry -> {
            if (now - entry.getValue() > DEBOUNCE_TICKS * 50L) {
                final UUID uuid = entry.getKey();
                plugin.getOnlineUser(uuid).ifPresent(user -> {
                    plugin.runAsync(() -> {
                        try {
                            final DataSnapshot.Packed snapshot = user.createSnapshot(
                                DataSnapshot.SaveCause.WORLD_SAVE);  // 复用 WORLD_SAVE 原因
                            plugin.getRedisManager().setUserData(user, snapshot);
                            plugin.debug("[%s] Real-time inventory synced to Redis".formatted(user.getName()));
                        } catch (Throwable e) {
                            plugin.log(Level.WARNING, "Failed to sync inventory to Redis", e);
                        }
                    });
                });
                return true;  // 移除已处理的条目
            }
            return false;
        });
    }

    public void initialize() { /* 注册事件 + 启动定时任务 */ }
    public void terminate() { /* 注销事件 + 停止定时任务 */ }
}
```

### 第二步：在 Settings 中新增实时同步配置

**文件**: [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java)

在 `SynchronizationSettings` 中新增：

```java
@Comment({"Real-time inventory synchronization settings.",
         "When enabled, all inventory changes are immediately synced to Redis."})
private InventorySyncSettings inventorySync = new InventorySyncSettings();

@Getter
@Configuration
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public static class InventorySyncSettings {
    @Comment("Enable real-time inventory sync to Redis (every click/drag/pickup)")
    private boolean enabled = true;

    @Comment("Debounce interval in ticks between syncs (default: 10 ticks = 500ms)")
    private long debounceTicks = 10;

    @Comment("Also sync ender chest contents in real-time")
    private boolean syncEnderChest = false;
}
```

### 第三步：在 BukkitHuskSync 中注册 InventorySyncListener

**文件**: [BukkitHuskSync.java](bukkit/src/main/java/net/william278/husksync/BukkitHuskSync.java)

在 `onEnable()` 的事件注册阶段之后：

```java
// Register inventory real-time sync listener
if (getSettings().getSynchronization().getInventorySync().isEnabled()) {
    initialize("inventory sync listener", (plugin) -> {
        this.inventorySyncListener = new InventorySyncListener(this);
        this.inventorySyncListener.initialize();
    });
}
```

新增字段：
```java
private InventorySyncListener inventorySyncListener;
```

在 `onDisable()` 中终止：
```java
if (this.inventorySyncListener != null) {
    this.inventorySyncListener.terminate();
}
```

### 第四步：优化现有流程确保一致性

**关键原则**：既然 Redis 是背包的唯一管理者，以下场景需要保证 Redis 数据不被覆盖或丢失：

#### 4.1 玩家加入时（已实现）

当前流程已经是：MySQL → Redis → Player，无需额外修改。

#### 4.2 saveCurrentUserData（物品变更保存）

当前已改为 **仅写 Redis**，与实时同步互补：
- 实时同步监听器处理**频繁的小操作**（点击、拖拽）
- `saveCurrentUserData(WORLD_SAVE)` 作为**周期性兜底**（世界保存时全量同步）

两者不冲突，都是写 Redis。

#### 4.3 玩家退出时（已实现）

当前已改为 **Redis + MySQL 双写**，退出时从 Redis（权威源）+ MySQL（持久化）都保存一份。

---

## 数据流总览（改造后）

```
┌──────────────────────────────────────────────────────────────┐
│                   玩家在线期间（实时）                         │
│                                                              │
│  [游戏内背包操作]                                             │
│    ├── 点击背包 (InventoryClickEvent)     ─┐                  │
│    ├── 拖拽物品 (InventoryDragEvent)      │                  │
│    ├── 拾取物品 (EntityPickupItemEvent)   ├─→ 防抖队列       │
│    └── 其他背包相关事件...                 │                  │
│                                         ↓ (每500ms)        │
│                              createSnapshot() → Redis.setUserData()
│                                              ↓               │
│                                    Redis 始终保持最新背包状态   │
│                                                              │
│  [周期性兜底] 世界保存事件 → saveCurrentUserData() → Redis     │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                   玩家离线时                                   │
│                                                              │
│  syncSaveUserData(DISCONNECT)                                 │
│       ↓                                                      │
│  Redis(当前权威数据) + MySQL(持久化快照) 双写                  │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                   玩家再次上线                                 │
│                                                              │
│  syncApplyUserData()                                          │
│       ↓                                                      │
│  ① MySQL.getLatestSnapshot() → 加载持久化数据                 │
│       ↓                                                      │
│  ② Redis.setUserData()     → 写入 Redis 缓存                 │
│       ↓                                                      │
│  ③ Redis.getUserDataNoConsume() → 应用到玩家                 │
│       ↓                                                      │
│  之后所有背包操作 → 回到实时同步循环                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 文件改动清单

| 文件 | 操作 | 改动说明 |
|------|------|----------|
| `.../listener/InventorySyncListener.java` | **新建** | 实时背包同步事件监听器（防抖+批量写入Redis） |
| [Settings.java](common/src/main/java/net/william278/husksync/config/Settings.java) | **修改** | 新增 InventorySyncSettings 配置组 |
| [BukkitHuskSync.java](bukkit/src/main/java/net/william278/husksync/BukkitHuskSync.java) | **修改** | 初始化/终止 InventorySyncListener |
| [EventListener.java](common/src/main/java/net/william278/husksync/listener/EventListener.java) | 无需修改 | 已在上轮改为 SHUTDOWN 双写路径 |

> 注：DataSyncer / LockstepDataSyncer / DelayDataSyncer / RedisManager / DatabaseBackupManager 保持上一轮改动不变。

---

## 注意事项

1. **性能影响**：高频事件监听 + 定时任务会带来一定 CPU 开销。防抖机制（默认 500ms）可有效缓解。对于大型服务器（100+ 在线），建议适当增大 debounce 间隔。
2. **线程安全**：`scheduleRedisSync()` 只写 ConcurrentHashMap（O(1)），不执行重操作。实际的序列化和 Redis 写入在异步线程完成，不影响主线程。
3. **与现有 WORLD_SAVE 保存的关系**：两者是**互补**关系——实时同步处理细粒度操作，WORLD_SAVE 作为周期性全量兜底。不会造成重复写入问题（都是写同一个 Redis key）。
4. **跨服切换**：跨服切换时 SERVER_SWITCH 标记机制不受影响，因为实时同步只在玩家稳定在线时工作。

---

## 验证步骤

1. 编译通过无错误
2. 启动服务器，确认日志显示 "inventory sync listener initialized"
3. 进入游戏，进行背包操作（移动物品、拾取掉落物等）
4. 观察 debug 日志确认 "Real-time inventory synced to Redis" 出现
5. 使用 Redis CLI 查看 `husksync:*:latest_snapshot:{uuid}` 确认数据更新
6. 退出服务器，确认 MySQL 有完整的离线快照
7. 重新加入，确认背包数据与退出前一致

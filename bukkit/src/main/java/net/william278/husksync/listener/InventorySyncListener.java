/*
 * This file is part of HuskSync, licensed under the Apache License 2.0.
 *
 *  Copyright (c) William278 <will27528@gmail.com>
 *  Copyright (c) contributors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.william278.husksync.listener;

import net.william278.husksync.BukkitHuskSync;
import net.william278.husksync.data.DataSnapshot;
import net.william278.husksync.user.BukkitUser;
import net.william278.husksync.user.OnlineUser;
import net.william278.husksync.util.Task;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.event.player.PlayerDropItemEvent;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Full Redis inventory takeover listener.
 *
 * <p>Intercepts all inventory change events and establishes a bidirectional control loop:
 * <ol>
 *   <li><b>Write phase</b>: Player action → intercept → createSnapshot → write to Redis</li>
 *   <li><b>Override phase</b>: Read from Redis → applySnapshot to player (force-overwrite)</li>
 *   <li><b>Enforcement phase</b>: Periodic task ensures all online players match Redis state</li>
 * </ol>
 *
 * <p>Redis becomes the single source of truth. The Minecraft game client is merely a display layer
 * that renders what Redis dictates. Any discrepancy is corrected by the override mechanism.
 */
public class InventorySyncListener implements Listener {

    private final BukkitHuskSync plugin;

    // Phase 1: Pending writes to Redis (debounced)
    private final Map<UUID, Long> pendingWrites;
    private Task.Repeating writeTask;

    // Phase 2: Pending overrides from Redis to player (debounced, after writes)
    private final Map<UUID, Long> pendingOverrides;
    private Task.Repeating overrideTask;

    // Phase 3: Periodic enforcement for all online players (safety net)
    private Task.Repeating enforceTask;

    // Anti-loop protection: players currently being overridden are ignored by event handlers
    private final Set<UUID> overridingPlayers;

    private final long debounceMillis;
    private final long enforceIntervalTicks;
    private boolean enabled;

    public InventorySyncListener(@NotNull BukkitHuskSync plugin) {
        this.plugin = plugin;
        this.pendingWrites = new ConcurrentHashMap<>();
        this.pendingOverrides = new ConcurrentHashMap<>();
        this.overridingPlayers = ConcurrentHashMap.newKeySet();
        this.debounceMillis = plugin.getSettings().getSynchronization()
                .getInventorySync().getDebounceTicks() * 50L; // ticks → ms
        this.enforceIntervalTicks = plugin.getSettings().getSynchronization()
                .getInventorySync().getEnforceIntervalSeconds() * 20L; // seconds → ticks
        this.enabled = false;
    }

    /**
     * Register Bukkit event listeners and start all periodic tasks.
     */
    public void initialize() {
        if (!plugin.getSettings().getSynchronization().getInventorySync().isEnabled()) {
            plugin.log(Level.INFO, "Full Redis inventory takeover is disabled in config");
            return;
        }

        // Register Bukkit event listeners
        plugin.getServer().getPluginManager().registerEvents(this, plugin);

        // Phase 1: Periodic task to process pending Redis writes (debounced)
        final long writeInterval = Math.max(1, plugin.getSettings().getSynchronization()
                .getInventorySync().getDebounceTicks());
        this.writeTask = plugin.getRepeatingTask(this::processPendingWrites, writeInterval);

        // Phase 2: Periodic task to process pending Redis→Player overrides (debounced + slight delay)
        final long overrideInterval = Math.max(writeInterval + 1, writeInterval + 1);
        this.overrideTask = plugin.getRepeatingTask(this::processPendingOverrides, overrideInterval);

        // Phase 3: Periodic enforcement task (safety net - force all players to match Redis)
        if (enforceIntervalTicks > 0) {
            this.enforceTask = plugin.getRepeatingTask(this::enforceAllFromRedis, enforceIntervalTicks);
        }

        this.enabled = true;
        plugin.log(Level.INFO, "Full Redis inventory takeover initialized "
                + "(debounce: %dms, enforce: every %ds)"
                .formatted(debounceMillis, enforceIntervalTicks / 20L));
    }

    /**
     * Unregister listeners and stop all periodic tasks.
     */
    public void terminate() {
        this.enabled = false;
        if (writeTask != null) { writeTask.cancel(); writeTask = null; }
        if (overrideTask != null) { overrideTask.cancel(); overrideTask = null; }
        if (enforceTask != null) { enforceTask.cancel(); enforceTask = null; }
        pendingWrites.clear();
        pendingOverrides.clear();
        overridingPlayers.clear();
    }

    // === Event Handlers ===

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onInventoryClick(@NotNull InventoryClickEvent event) {
        if (!(event.getWhoClicked() instanceof Player player)) return;
        if (shouldIgnorePlayer(player)) return;
        scheduleFullSync(BukkitUser.adapt(player, plugin));
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onInventoryDrag(@NotNull InventoryDragEvent event) {
        if (!(event.getWhoClicked() instanceof Player player)) return;
        if (shouldIgnorePlayer(player)) return;
        scheduleFullSync(BukkitUser.adapt(player, plugin));
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onEntityPickupItem(@NotNull EntityPickupItemEvent event) {
        if (!(event.getEntity() instanceof Player player)) return;
        if (shouldIgnorePlayer(player)) return;
        scheduleFullSync(BukkitUser.adapt(player, plugin));
    }

    @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true)
    public void onPlayerDropItem(@NotNull PlayerDropItemEvent event) {
        if (shouldIgnorePlayer(event.getPlayer())) return;
        scheduleFullSync(BukkitUser.adapt(event.getPlayer(), plugin));
    }

    // === Bidirectional Sync Logic ===

    /**
     * Schedule a full bidirectional sync cycle for the given user:
     * - Phase 1: Write current inventory state to Redis (after debounce)
     * - Phase 2: Override player inventory from Redis (after write completes + debounce)
     */
    private void scheduleFullSync(@NotNull OnlineUser user) {
        if (!enabled) return;
        final long now = System.currentTimeMillis();
        pendingWrites.put(user.getUuid(), now);
        pendingOverrides.put(user.getUuid(), now);
    }

    /**
     * Process Phase 1: Write pending player states to Redis.
     * Entries that have exceeded the debounce interval will have their current snapshot written to Redis.
     */
    private void processPendingWrites() {
        if (!enabled || pendingWrites.isEmpty()) return;

        final long now = System.currentTimeMillis();
        pendingWrites.entrySet().removeIf(entry -> {
            if (now - entry.getValue() >= debounceMillis) {
                final UUID uuid = entry.getKey();
                plugin.getOnlineUser(uuid).ifPresent(user -> {
                    plugin.runAsync(() -> writeToRedis(user));
                });
                return true;
            }
            return false;
        });
    }

    /**
     * Process Phase 2: Override player inventories from Redis.
     * Entries that have exceeded the debounce interval (+ buffer) will be force-applied from Redis data.
     * This ensures the player's inventory always matches Redis exactly.
     */
    private void processPendingOverrides() {
        if (!enabled || pendingOverrides.isEmpty()) return;

        final long now = System.currentTimeMillis();
        // Use a slightly longer interval than writes to ensure write happens first
        final long overrideThreshold = debounceMillis + 50L; // +50ms buffer

        pendingOverrides.entrySet().removeIf(entry -> {
            if (now - entry.getValue() >= overrideThreshold) {
                final UUID uuid = entry.getKey();
                plugin.getOnlineUser(uuid).ifPresent(user -> {
                    plugin.runAsync(() -> overrideFromRedis(user));
                });
                return true;
            }
            return false;
        });
    }

    /**
     * Phase 3: Periodic enforcement - force all online players' inventories to match Redis.
     * Acts as a safety net to correct any discrepancies from non-event sources
     * (other plugins, cheats, direct API calls, etc.)
     */
    private void enforceAllFromRedis() {
        if (!enabled) return;
        plugin.getOnlineUsers().forEach(user -> {
            if (plugin.isLocked(user.getUuid())) return;
            if (overridingPlayers.contains(user.getUuid())) return; // Already being overridden
            plugin.runAsync(() -> overrideFromRedis(user));
        });
    }

    // === Core Operations ===

    /**
     * Write a user's current inventory state to Redis.
     */
    private void writeToRedis(@NotNull OnlineUser user) {
        try {
            if (user.hasDisconnected()) return;
            final DataSnapshot.Packed snapshot = user.createSnapshot(DataSnapshot.SaveCause.WORLD_SAVE);
            plugin.getRedisManager().setUserData(user, snapshot);
            plugin.debug("[%s] [Takeover] Inventory synced TO Redis".formatted(user.getName()));
        } catch (Throwable e) {
            plugin.log(Level.WARNING, "[Takeover] Failed to sync %s inventory to Redis"
                    .formatted(user.getName()), e);
        }
    }

    /**
     * Override a user's inventory FROM Redis data.
     * This is the core of the "full takeover" - Redis dictates what the player sees.
     */
    private void overrideFromRedis(@NotNull OnlineUser user) {
        try {
            if (user.hasDisconnected()) return;

            // Anti-loop: mark this player as being overridden so events triggered by
            // applySnapshot are ignored by our handlers
            overridingPlayers.add(user.getUuid());
            try {
                plugin.getRedisManager().getUserDataNoConsume(user).ifPresent(snapshot -> {
                    user.applySnapshot(snapshot, DataSnapshot.UpdateCause.SYNCHRONIZED);
                    plugin.debug("[%s] [Takeover] Inventory overridden FROM Redis".formatted(user.getName()));
                });
            } finally {
                overridingPlayers.remove(user.getUuid());
            }
        } catch (Throwable e) {
            plugin.log(Level.WARNING, "[Takeover] Failed to override %s inventory from Redis"
                    .formatted(user.getName()), e);
            overridingPlayers.remove(user.getUuid()); // Ensure cleanup on error
        }
    }

    /**
     * Check if a player's inventory events should be ignored.
     * Ignores players who are:
     * - Currently locked by HuskSync (during data synchronization)
     * - Currently being overridden from Redis (anti-loop protection)
     */
    private boolean shouldIgnorePlayer(@NotNull Player player) {
        return plugin.isLocked(player.getUniqueId())
                || overridingPlayers.contains(player.getUniqueId());
    }

    /**
     * Check if the listener is currently active.
     */
    public boolean isEnabled() {
        return enabled;
    }
}

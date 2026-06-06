<p align="center">
    <h1>GroupPack</h1>
    <strong>Redis-managed player inventory system with MySQL persistence and auto-backup</strong>
    <br/>
    <br/>
    <a href="#features">Features</a> &middot;
    <a href="#architecture">Architecture</a> &middot;
    <a href="#setup">Setup</a> &middot;
    <a href="#configuration">Configuration</a>
</p>

---

## Overview

**GroupPack** is a Bukkit/Paper plugin that puts **Redis in full control of player inventory data**. The game client becomes a mere display layer — Redis is the single source of truth for every player's backpack, Ender Chest, and all synchronized data.

### Core Design Philosophy

```
Player Action (click/drag/pickup/drop)
        |
        v
  [Intercept Event] ──→ Write to Redis (debounced)
        |                      |
        |               (delay ~500ms)
        |                      |
        v                      v
  [Read from Redis] ←── applySnapshot(Player)
        |
        v
  Player sees exactly what Redis dictates

  Periodic enforcement (every N seconds):
    All online players → Redis → Player (safety net)

  On disconnect:
    Redis + MySQL dual-write (persistence)

  On next join:
    MySQL → Redis → Player (restore from persistent store)
```

## Features

- **Full Redis Inventory Takeover** — Every inventory operation (click, drag, pickup, drop) is intercepted and synced to Redis in real-time. The player's inventory is then force-overwritten from Redis, ensuring the game never diverges from the authoritative data.

- **Bidirectional Control Loop** — Three-phase sync: (1) write to Redis, (2) override player from Redis, (3) periodic enforcement as safety net. Anti-loop protection prevents infinite event cascades.

- **MySQL Persistence on Disconnect** — Player data is only persisted to MySQL when the player goes offline or the server shuts down. Online changes stay in Redis for maximum performance.

- **Auto MySQL Backup** — Configurable automatic database backups via `mysqldump`. Set your own interval, backup directory, and retention policy right in `config.yml`.

- **Cross-Server Synchronization** — Built on HuskSync's proven infrastructure, supports both LOCKSTEP and DELAY synchronization modes for proxy networks (BungeeCord, Velocity, etc.).

- **Debounced Real-Time Sync** — High-frequency inventory events are batched with a configurable debounce interval (default 500ms) to prevent Redis flooding and maintain server performance.

- **Complete Data Sync** — Inventories, Ender Chests, health, hunger, potion effects, advancements, statistics, game mode, flight status, experience, location, and persistent data.

## Architecture

| Component | Role |
|-----------|------|
| **Redis** | Primary cache & authoritative inventory store. All reads/writes during gameplay go through Redis. |
| **MySQL / MariaDB** | Persistent storage. Written to only on disconnect/shutdown. Loaded from on join. |
| **InventorySyncListener** | Bukkit event interceptor. Captures all inventory operations and drives the bidirectional sync loop. |
| **DatabaseBackupManager** | Scheduled backup engine using mysqldump. Fully configurable interval and retention. |
| **DataSyncer** | Orchestrates join/quit flows: MySQL→Redis→Player on join, Redis+MySQL on quit. |

## Setup

### Requirements

- **Minecraft**: Paper 1.21.x (Java 21)
- **Redis**: v5.0+ (required)
- **MySQL / MariaDB**: required for persistence
- **mysqldump**: installed on system PATH (for auto-backup feature)

### Installation

1. Place the `GroupPack.jar` file in the `/plugins` directory of each server.
2. Start the server, then stop it to let GroupPack generate the config files.
3. Edit `plugins/GroupPack/config.yml` and fill in:
   - Database credentials (under `database` section)
   - Redis connection details (under `redis` section)
4. Restart the server. GroupPack will initialize and begin managing inventories.

### Quick Config

```yaml
# config.yml (key sections)

database:
  type: MYSQL          # or MARIADB
  credentials:
    host: localhost
    port: 3306
    database: grouppack
    username: root
    password: your_password

redis:
  host: localhost
  port: 6379
  password: ""         # leave empty if no auth

synchronization:
  mode: LOCKSTEP       # or DELAY

  # === Real-time inventory takeover ===
  inventory-sync:
    enabled: true                    # Enable full Redis takeover
    debounce-ticks: 10              # Debounce interval (500ms)
    enforce-interval-seconds: 5     # Periodic Redis→Player enforcement (0=disable)

  # === Auto MySQL Backup ===
  backup:
    enabled: true
    interval-minutes: 360           # Backup every 6 hours
    backup-directory: "backups"
    max-backups: 30                 # Keep last 30 backups
```

## Configuration Reference

### `inventory-sync` Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable full Redis inventory takeover mode |
| `debounce-ticks` | long | `10` | Milliseconds between batched syncs (lower = more responsive, higher CPU) |
| `enforce-interval-seconds` | long | `5` | How often all players are force-synced from Redis. Set to `0` to disable |

### `backup` Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable automatic MySQL database backups |
| `interval-minutes` | long | `360` | Time between backups in minutes |
| `backup-directory` | string | `"backups"` | Subdirectory under plugin folder for backup SQL files |
| `max-backups` | int | `30` | Maximum number of backup files to keep (`0` = unlimited) |

### Synchronization Modes

| Mode | Behavior |
|------|----------|
| **LOCKSTEP** | Strict ordering: servers coordinate via Redis checkout locks before applying data. Recommended for most networks. |
| **DELAY** | Relaxed: waits for network latency duration before loading data. Simpler but less consistent under rapid switching. |

## Development

Build requires Java 21:

```bash
./gradlew clean build
```

Output JAR will be in `bukkit/build/libs/`.

## License

Based on [HuskSync](https://github.com/WiIIiam278/HuskSync) by William278, licensed under the Apache 2.0 License.

---

*GroupPack — Redis owns the inventory. The game just displays it.*

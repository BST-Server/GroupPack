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

package net.william278.husksync.database;

import net.william278.husksync.HuskSync;
import net.william278.husksync.config.Settings;
import net.william278.husksync.util.Task;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.logging.Level;

/**
 * Manages scheduled MySQL database backups using mysqldump.
 */
public class DatabaseBackupManager {

    private static final DateTimeFormatter BACKUP_FILE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final String BACKUP_FILE_PREFIX = "husksync_backup_";
    private static final String BACKUP_FILE_SUFFIX = ".sql";

    private final HuskSync plugin;
    private Task.Repeating backupTask;
    private boolean enabled;

    public DatabaseBackupManager(@NotNull HuskSync plugin) {
        this.plugin = plugin;
        this.enabled = false;
    }

    /**
     * Initialize the backup manager: check mysqldump availability, create backup directory,
     * and schedule periodic backup tasks if enabled in config.
     */
    @Blocking
    public void initialize() {
        final Settings.SynchronizationSettings.BackupSettings backupSettings =
                plugin.getSettings().getSynchronization().getBackup();

        if (!backupSettings.isEnabled()) {
            plugin.log(Level.INFO, "MySQL database auto-backup is disabled in config");
            return;
        }

        // Check that the database type supports mysqldump (MySQL/MariaDB only)
        final Database.Type dbType = plugin.getSettings().getDatabase().getType();
        if (dbType != Database.Type.MYSQL && dbType != Database.Type.MARIADB) {
            plugin.log(Level.WARNING, "MySQL auto-backup is only supported for MySQL/MariaDB databases. "
                    + "Current type: %s".formatted(dbType.getDisplayName()));
            return;
        }

        // Verify mysqldump is available on the system
        if (!isMysqldumpAvailable()) {
            plugin.log(Level.WARNING, "mysqldump command not found on system PATH. "
                    + "Auto-backup will be disabled. Please install MySQL client tools.");
            return;
        }

        // Ensure backup directory exists
        final Path backupDir = getBackupDirectory();
        try {
            Files.createDirectories(backupDir);
            plugin.log(Level.INFO, "Backup directory created: %s".formatted(backupDir.toAbsolutePath()));
        } catch (IOException e) {
            plugin.log(Level.SEVERE, "Failed to create backup directory: %s"
                    .formatted(backupDir.toAbsolutePath()), e);
            return;
        }

        // Schedule periodic backup task
        final long intervalMinutes = backupSettings.getIntervalMinutes();
        final long intervalTicks = (intervalMinutes * 60L) / 50L; // Convert minutes to ticks (~20 ticks/sec)

        this.backupTask = plugin.getRepeatingTask(() -> {
            plugin.runAsync(this::executeBackup);
        }, Math.max(1, intervalTicks)); // At least 1 tick

        this.enabled = true;
        plugin.log(Level.INFO, "MySQL database auto-backup scheduled every %d minutes "
                + "(next backup in %s)".formatted(intervalMinutes, backupDir.toAbsolutePath()));
    }

    /**
     * Execute a single database backup operation using mysqldump.
     */
    @Blocking
    public void executeBackup() {
        if (!enabled) {
            return;
        }

        final Settings.DatabaseSettings.DatabaseCredentials credentials =
                plugin.getSettings().getDatabase().getCredentials();
        final Settings.SynchronizationSettings.BackupSettings backupSettings =
                plugin.getSettings().getSynchronization().getBackup();
        final Path backupFile = generateBackupFilePath();

        plugin.log(Level.INFO, "Starting MySQL database backup to: %s".formatted(backupFile.getFileName()));

        try {
            // Build mysqldump command
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "mysqldump",
                    "-h", credentials.getHost(),
                    "-P", String.valueOf(credentials.getPort()),
                    "-u", credentials.getUsername(),
                    "-p" + credentials.getPassword(),
                    "--single-transaction",
                    "--routines",
                    "--triggers",
                    "--databases", credentials.getDatabase()
            );
            processBuilder.redirectErrorStream(true);
            processBuilder.redirectOutput(backupFile.toFile());

            final Process process = processBuilder.start();
            final int exitCode = process.waitFor();

            if (exitCode == 0) {
                final long fileSizeKB = Files.size(backupFile) / 1024;
                plugin.log(Level.INFO, "Database backup completed successfully: %s (%d KB)"
                        .formatted(backupFile.getFileName(), fileSizeKB));

                // Clean up old backups
                cleanupOldBackups();
            } else {
                plugin.log(Level.WARNING, "mysqldump exited with code %d. Backup may have failed."
                        .formatted(exitCode));
                // Delete partial/failed backup file
                try {
                    Files.deleteIfExists(backupFile);
                } catch (IOException ignored) {
                    // Ignore cleanup failure
                }
            }
        } catch (IOException | InterruptedException e) {
            plugin.log(Level.SEVERE, "Failed to execute database backup", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Clean up old backup files exceeding the maxBackups limit configured in settings.
     */
    @Blocking
    private void cleanupOldBackups() {
        final int maxBackups = plugin.getSettings().getSynchronization().getBackup().getMaxBackups();
        if (maxBackups <= 0) {
            return; // Unlimited
        }

        final Path backupDir = getBackupDirectory();
        try {
            final var backupFiles = Files.list(backupDir)
                    .filter(p -> p.getFileName().toString().startsWith(BACKUP_FILE_PREFIX)
                            && p.getFileName().toString().endsWith(BACKUP_FILE_SUFFIX))
                    .sorted(Comparator.comparingLong(p -> {
                        try {
                            return Files.getLastModifiedTime(p).toMillis();
                        } catch (IOException e) {
                            return Long.MAX_VALUE;
                        }
                    }))
                    .toList();

            final int toDelete = backupFiles.size() - maxBackups;
            if (toDelete > 0) {
                for (int i = 0; i < toDelete; i++) {
                    final Path oldBackup = backupFiles.get(i);
                    try {
                        Files.delete(oldBackup);
                        plugin.log(Level.INFO, "Deleted old backup: %s".formatted(oldBackup.getFileName()));
                    } catch (IOException e) {
                        plugin.log(Level.WARNING, "Failed to delete old backup: %s"
                                .formatted(oldBackup.getFileName()));
                    }
                }
            }
        } catch (IOException e) {
            plugin.log(Level.WARNING, "Failed to list backup files for cleanup", e);
        }
    }

    /**
     * Generate the file path for the next backup file based on current timestamp.
     */
    @NotNull
    private Path generateBackupFilePath() {
        final String timestamp = LocalDateTime.now().format(BACKUP_FILE_FORMAT);
        return getBackupDirectory().resolve(BACKUP_FILE_PREFIX + timestamp + BACKUP_FILE_SUFFIX);
    }

    /**
     * Get the backup directory path from config settings.
     */
    @NotNull
    private Path getBackupDirectory() {
        final String dirName = plugin.getSettings().getSynchronization().getBackup().getBackupDirectory();
        return plugin.getConfigDirectory().resolve(dirName);
    }

    /**
     * Check if the mysqldump command is available on the system PATH.
     *
     * @return true if mysqldump can be executed
     */
    private boolean isMysqldumpAvailable() {
        try {
            final Process process = new ProcessBuilder("mysqldump", "--version")
                    .redirectErrorStream(true)
                    .start();
            final boolean finished = process.waitFor() == 0;
            if (finished) {
                final String versionInfo = new String(process.getInputStream().readAllBytes());
                plugin.log(Level.INFO, "Found mysqldump: %s"
                        .formatted(versionInfo.lines().findFirst().map(String::trim).orElse("unknown version")));
            }
            return finished;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    /**
     * Terminate the backup manager and cancel scheduled tasks.
     */
    public void terminate() {
        this.enabled = false;
        if (backupTask != null) {
            backupTask.cancel();
            backupTask = null;
            plugin.log(Level.INFO, "Database backup manager terminated");
        }
    }

    /**
     * Check if the backup manager is currently active.
     */
    public boolean isEnabled() {
        return enabled;
    }
}

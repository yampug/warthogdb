const std = @import("std");
const log_file = @import("log.zig");
const tombstone_file = @import("tombstone.zig");
const tombstone_log = @import("log.zig");
const index_module = @import("index.zig");
const index_file = @import("index_file.zig");
const record = @import("record.zig");
const metadata = @import("metadata.zig");
pub const snapshot = @import("snapshot.zig");
const Allocator = std.mem.Allocator;

pub const WarthogDBOptions = struct {
    max_file_size: u64 = 1024 * 1024 * 1024,
    max_tombstone_file_size: u64 = 0,
    compaction_threshold_per_file: f64 = 0.75,
    fixed_key_size: usize = 255,
    sync_write: bool = false,
    number_of_records: usize = 1_000_000,
};

pub const WarthogDB = struct {
    allocator: Allocator,
    options: WarthogDBOptions,
    dir: std.fs.Dir,
    index: index_module.OffHeapHashTable,

    current_write_file: ?*log_file.LogFile = null,
    current_tombstone_file: ?log_file.TombstoneFile = null,

    lock_file: ?std.fs.File = null,

    read_files: std.AutoHashMap(u32, *log_file.LogFile),
    files_lock: std.Thread.RwLock = .{},

    metadata: metadata.MetaData,

    // Concurrency and Compaction
    mutex: std.Thread.Mutex = .{},
    stale_data_map: std.AutoHashMap(u32, u64),
    compaction_queue: std.ArrayListUnmanaged(u32) = .{},
    compaction_write_file: ?*log_file.LogFile = null,
    next_file_id: u32 = 0,
    compaction_thread: ?std.Thread = null,

    sequence_number: u64 = 0,

    pub fn init(allocator: Allocator, dir_path: []const u8, options: WarthogDBOptions) !WarthogDB {
        const dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });

        const lock_path = try std.fs.path.join(allocator, &[_][]const u8{ dir_path, "LOCK" });
        defer allocator.free(lock_path);
        const lock_file = try std.fs.cwd().createFile(lock_path, .{ .read = true, .truncate = false });
        // Initialize index with reasonable segment count
        const index = try index_module.OffHeapHashTable.init(allocator, 256);

        return .{
            .allocator = allocator,
            .options = options,
            .dir = dir,
            .index = index,
            .lock_file = lock_file,
            .read_files = std.AutoHashMap(u32, *log_file.LogFile).init(allocator),
            .stale_data_map = std.AutoHashMap(u32, u64).init(allocator),
            .compaction_queue = .{},
            .metadata = metadata.MetaData.init(),
            .sequence_number = 0,
            .next_file_id = 0,
            .compaction_thread = null,
        };
    }

    pub fn getRef(self: *WarthogDB, key: []const u8, allocator: Allocator) !?record.RecordView {
        if (self.index.get(key)) |entry| {
            var file_ptr: ?*log_file.LogFile = null;

            self.files_lock.lockShared();
            if (self.read_files.get(entry.file_id)) |f| {
                file_ptr = f;
            }
            self.files_lock.unlockShared();

            if (file_ptr == null) {
                if (self.current_write_file) |f| {
                    if (f.file_id == entry.file_id) {
                        file_ptr = f;
                    }
                }
            }

            if (file_ptr) |f| {
                const record_start_offset = entry.value_offset - key.len - record.RecordHeader.SIZE;
                const rec_view = try f.readRecordView(record_start_offset, allocator);

                if (!std.mem.eql(u8, rec_view.key, key)) {
                    // Allocator might have been used if fallback
                    rec_view.deinit();
                    return error.KeyMismatch;
                }
                return rec_view;
            }
            return error.FileNotFound;
        }
        return null;
    }

    pub fn get(self: *WarthogDB, key: []const u8, allocator: Allocator) !?[]u8 {
        if (self.index.get(key)) |entry| {
            var file_ptr: ?*log_file.LogFile = null;

            self.files_lock.lockShared();
            if (self.read_files.get(entry.file_id)) |f| {
                file_ptr = f;
            }
            self.files_lock.unlockShared();

            if (file_ptr == null) {
                if (self.current_write_file) |f| {
                    if (f.file_id == entry.file_id) {
                        file_ptr = f;
                    }
                }
            }

            if (file_ptr) |f| {
                const record_start_offset = entry.value_offset - key.len - record.RecordHeader.SIZE;
                const rec_view = try f.readRecordView(record_start_offset, allocator);
                defer rec_view.deinit();

                if (!std.mem.eql(u8, rec_view.key, key)) {
                    std.debug.print("Key mismatch! Wanted: {s}, Found: {s} at offset {d} in file {d}\n", .{ key, rec_view.key, record_start_offset, entry.file_id });
                    return error.KeyMismatch;
                }

                return try allocator.dupe(u8, rec_view.value);
            }
            return error.FileNotFound;
        }
        return null;
    }

    pub fn getFromSnapshot(self: *WarthogDB, snap: *snapshot.Snapshot, key: []const u8, allocator: Allocator) !?[]u8 {
        if (self.index.get(key)) |entry| {
            // Check if file is in snapshot
            if (snap.getFile(entry.file_id)) |f| {
                const record_start_offset = entry.value_offset - key.len - record.RecordHeader.SIZE;
                const rec_view = try f.readRecordView(record_start_offset, allocator);
                defer rec_view.deinit();

                if (!std.mem.eql(u8, rec_view.key, key)) {
                    return error.KeyMismatch;
                }

                return try allocator.dupe(u8, rec_view.value);
            }
            // File not in snapshot (created later), effectively not visible in consistent view relative to FILE SET.
            return error.FileNotFound;
        }
        return null;
    }

    pub fn openSnapshot(self: *WarthogDB) !*snapshot.Snapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const snap = try self.allocator.create(snapshot.Snapshot);
        snap.* = snapshot.Snapshot.init(self.allocator);

        // Add all current read files
        self.files_lock.lockShared();
        var it = self.read_files.iterator();
        while (it.next()) |entry| {
            try snap.addFile(entry.value_ptr.*);
        }
        self.files_lock.unlockShared();

        // Add current write file if exists
        if (self.current_write_file) |f| {
            try snap.addFile(f);
        }

        return snap;
    }

    pub fn closeSnapshot(self: *WarthogDB, snap: *snapshot.Snapshot) void {
        snap.deinit();
        self.allocator.destroy(snap);
    }

    pub fn close(self: *WarthogDB) void {
        self.metadata.open = false;
        if (self.compaction_thread) |t| {
            t.join();
        }

        self.flushMetadata() catch |err| {
            std.debug.print("Failed to flush metadata on close: {}\n", .{err});
        };

        if (self.current_write_file) |f| {
            f.close();
            self.allocator.destroy(f);
        }

        var it = self.read_files.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.close();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.read_files.deinit();
        self.stale_data_map.deinit();

        self.index.deinit();

        if (self.current_tombstone_file) |*f| {
            f.close();
        }

        if (self.lock_file) |f| {
            f.unlock();
            f.close();
        }
    }

    pub fn deinit(self: *WarthogDB) void {
        if (self.current_write_file) |f| {
            f.close();
            self.allocator.destroy(f);
        }
        if (self.current_tombstone_file) |*f| {
            f.close();
        }
        if (self.compaction_write_file) |f| {
            f.close();
            self.allocator.destroy(f);
        }
        var iter = self.read_files.valueIterator();
        while (iter.next()) |f| {
            f.*.close();
            self.allocator.destroy(f.*);
        }
        self.read_files.deinit();
        self.stale_data_map.deinit();
        self.compaction_queue.deinit(self.allocator);
        self.index.deinit();

        if (self.lock_file) |f| {
            f.unlock();
            f.close();
        }
        self.dir.close();
    }

    pub fn open(allocator: Allocator, dir_path: []const u8, options: WarthogDBOptions) !WarthogDB {
        std.fs.cwd().makeDir(dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        var instance = try init(allocator, dir_path, options);
        errdefer instance.deinit();

        var meta = metadata.MetaData.init();
        const meta_path = try std.fs.path.join(allocator, &[_][]const u8{ dir_path, metadata.MetaData.METADATA_FILE_NAME });
        defer allocator.free(meta_path);

        const file = std.fs.cwd().openFile(meta_path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };

        if (file) |f| {
            defer f.close();
            var buf: [metadata.MetaData.SIZE]u8 = undefined;
            const bytes_read = try f.readAll(&buf);
            if (bytes_read == metadata.MetaData.SIZE) {
                meta = metadata.MetaData.deserialize(&buf);
                if (meta.max_file_size != 0 and meta.max_file_size != options.max_file_size) {
                    std.debug.print("Meta max_file_size {d} != options {d}\n", .{ meta.max_file_size, options.max_file_size });
                    return error.InvalidOptions;
                }
            }
        } else {
            meta.max_file_size = @intCast(options.max_file_size);
        }
        instance.metadata = meta;

        try instance.recover();

        if (instance.current_write_file == null) {
            const write_file = try log_file.LogFile.open(instance.dir, 1, true);
            const wf_ptr = try instance.allocator.create(log_file.LogFile);
            wf_ptr.* = write_file;
            instance.current_write_file = wf_ptr;
        }

        instance.metadata.open = true;
        try instance.flushMetadata();

        // Compaction thread moved to startCompaction() to ensure stable self pointer
        // instance.compaction_thread = try std.Thread.spawn(.{}, runCompaction, .{&instance});

        return instance;
    }

    pub fn startCompaction(self: *WarthogDB) !void {
        if (self.compaction_thread == null) {
            self.compaction_thread = try std.Thread.spawn(.{}, runCompaction, .{self});
        }
    }

    fn flushMetadata(self: *WarthogDB) !void {
        var buf: [metadata.MetaData.SIZE]u8 = undefined;
        self.metadata.serialize(&buf);
        const temp_name = metadata.MetaData.METADATA_FILE_NAME ++ ".temp";
        const temp_file = try self.dir.createFile(temp_name, .{ .read = true });
        defer temp_file.close();
        try temp_file.writeAll(&buf);
        try self.dir.rename(temp_name, metadata.MetaData.METADATA_FILE_NAME);
    }

    fn recover(self: *WarthogDB) !void {
        var dir_iter = self.dir.iterate();
        var max_file_id: u32 = 0;
        var max_seq: u64 = self.metadata.sequence_number;

        while (try dir_iter.next()) |entry| {
            if (entry.kind == .file) {
                if (std.mem.endsWith(u8, entry.name, ".data")) {
                    const id_str = entry.name[0 .. entry.name.len - 5];
                    const file_id = std.fmt.parseInt(u32, id_str, 10) catch continue;

                    if (file_id > max_file_id) max_file_id = file_id;

                    var file = try log_file.LogFile.open(self.dir, file_id, false);
                    var iter = file.iterator(self.allocator);
                    while (try iter.next()) |rec| {
                        defer self.allocator.free(rec.key);
                        defer self.allocator.free(rec.value);

                        if (rec.header.sequence_number > max_seq) {
                            max_seq = rec.header.sequence_number;
                        }

                        const record_offset = iter.current_offset - record.RecordHeader.SIZE - rec.key.len - rec.value.len;

                        const idx_entry = index_module.IndexEntry{
                            .file_id = file_id,
                            .value_offset = @intCast(record_offset + record.RecordHeader.SIZE + rec.key.len),
                            .value_size = @intCast(rec.value.len),
                            .sequence_number = rec.header.sequence_number,
                        };
                        try self.index.put(rec.key, idx_entry);
                    }
                    const file_ptr = try self.allocator.create(log_file.LogFile);
                    file_ptr.* = file;
                    try self.read_files.put(file_id, file_ptr);
                }
            }
        }
        self.sequence_number = max_seq;

        if (max_file_id == 0) {
            const timestamp = std.time.timestamp();
            max_file_id = @intCast(timestamp);
        }
        self.next_file_id = max_file_id + 1;

        if (max_file_id > 0) {
            if (self.read_files.get(max_file_id)) |f| {
                // Promote existing file to write file
                _ = self.read_files.remove(max_file_id);
                self.current_write_file = f;
            } else {
                // Create new write file
                const write_file = try log_file.LogFile.open(self.dir, max_file_id, true);
                const wf_ptr = try self.allocator.create(log_file.LogFile);
                wf_ptr.* = write_file;
                self.current_write_file = wf_ptr;
            }
        }

        dir_iter.reset();
        while (try dir_iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".tombstone")) {
                const id_str = entry.name[0 .. entry.name.len - 10];
                const file_id = std.fmt.parseInt(u32, id_str, 10) catch continue;

                var file = try log_file.TombstoneFile.open(self.dir, file_id, false);
                var offset: u64 = 0;
                const stat = try file.file.stat();
                while (offset < stat.size) {
                    const entry_res = file.readEntry(offset, self.allocator);
                    if (entry_res) |rec| {
                        defer self.allocator.free(rec.key);
                        _ = self.index.remove(rec.key);
                        offset += tombstone_file.TombstoneHeader.SIZE + rec.key.len;
                    } else |_| {
                        break;
                    }
                }
                file.close();
            }
        }
    }

    pub const Reader = struct {
        db: *WarthogDB,
        file_cache: std.AutoHashMap(u32, *log_file.LogFile),
        allocator: Allocator,
        last_file_id: ?u32 = null,
        last_file_ptr: ?*log_file.LogFile = null,

        pub fn init(db: *WarthogDB, allocator: Allocator) Reader {
            return .{
                .db = db,
                .file_cache = std.AutoHashMap(u32, *log_file.LogFile).init(allocator),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Reader) void {
            self.file_cache.deinit();
        }

        pub fn getRef(self: *Reader, key: []const u8, allocator: Allocator) !?record.RecordView {
            if (self.db.index.get(key)) |entry| {
                var file_ptr: ?*log_file.LogFile = null;

                // 0. Check fast path (last used file)
                if (self.last_file_id) |lid| {
                    if (lid == entry.file_id) {
                        file_ptr = self.last_file_ptr;
                    }
                }

                if (file_ptr == null) {
                    // 1. Check local cache
                    if (self.file_cache.get(entry.file_id)) |f| {
                        file_ptr = f;
                    } else {
                        // 2. Check DB (Shared Lock)
                        self.db.files_lock.lockShared();
                        if (self.db.read_files.get(entry.file_id)) |f| {
                            file_ptr = f;
                            // Cache it for future (Note: We assume file pointer remains valid until DB close)
                            // Ideally we should increment refcount here if we want to survive compaction deleting it from DB
                            // For now, this optimization relies on DB handling cleanup safely (e.g. epoch or blocking destroy until all readers done)
                            self.file_cache.put(entry.file_id, f) catch {};
                        }
                        self.db.files_lock.unlockShared();
                    }

                    if (file_ptr) |f| {
                        // Update fast path
                        self.last_file_id = entry.file_id;
                        self.last_file_ptr = f;
                    }
                }

                if (file_ptr == null) {
                    // 3. Last resort: specific check for current write file (lockless path attempt or locked?)
                    // The main getRef does check current_write_file.
                    self.db.files_lock.lockShared();
                    if (self.db.read_files.get(entry.file_id)) |f| {
                        file_ptr = f;
                    } else if (self.db.current_write_file) |f| {
                        if (f.file_id == entry.file_id) {
                            file_ptr = f;
                        }
                    }
                    self.db.files_lock.unlockShared();
                }

                if (file_ptr) |f| {
                    const record_start_offset = entry.value_offset - key.len - record.RecordHeader.SIZE;
                    const rec_view = try f.readRecordView(record_start_offset, allocator);

                    if (!std.mem.eql(u8, rec_view.key, key)) {
                        rec_view.deinit();
                        return error.KeyMismatch;
                    }
                    return rec_view;
                }
                return error.FileNotFound;
            }
            return null;
        }
    };

    pub fn reader(self: *WarthogDB, allocator: Allocator) Reader {
        return Reader.init(self, allocator);
    }
    pub fn put(self: *WarthogDB, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.sequence_number += 1;
        const seq = self.sequence_number;
        var rec = try record.Record.init(key, value, seq, 0);

        if (self.current_write_file == null) return error.NotOpen;

        const offset = try self.current_write_file.?.writeRecord(&rec);

        const file_id = self.current_write_file.?.file_id;
        const entry = index_module.IndexEntry{
            .file_id = file_id,
            .value_offset = @intCast(offset + record.RecordHeader.SIZE + key.len),
            .value_size = @intCast(value.len),
            .sequence_number = seq,
        };

        if (self.index.get(key)) |old_entry| {
            const stale_size = record.RecordHeader.SIZE + key.len + old_entry.value_size;
            try self.markFileAsStale(old_entry.file_id, stale_size);
        }

        try self.index.put(key, entry);

        if (self.current_write_file.?.write_offset > self.options.max_file_size) {
            try self.rollOverWriteFile();
        }
    }

    fn rollOverWriteFile(self: *WarthogDB) !void {
        if (self.current_write_file) |f| {
            // Enable mmap for the file transitioning to read-only
            f.enableMmap() catch |err| {
                std.debug.print("Failed to mmap rolled file {d}: {}\n", .{ f.file_id, err });
            };

            self.files_lock.lock(); // Exclusive lock for modification
            try self.read_files.put(f.file_id, f);
            self.files_lock.unlock();

            const new_id = self.getNextFileId();
            const write_file = try log_file.LogFile.open(self.dir, new_id, true);
            const wf_ptr = try self.allocator.create(log_file.LogFile);
            wf_ptr.* = write_file;
            self.current_write_file = wf_ptr;
        }
    }

    fn markFileAsStale(self: *WarthogDB, file_id: u32, size: u64) !void {
        const current = self.stale_data_map.get(file_id) orelse 0;
        const new_stale = current + size;
        try self.stale_data_map.put(file_id, new_stale);

        const threshold = self.options.compaction_threshold_per_file;
        const max_size = self.options.max_file_size;

        if (@as(f64, @floatFromInt(new_stale)) > threshold * @as(f64, @floatFromInt(max_size))) {
            var found = false;
            for (self.compaction_queue.items) |fid| {
                if (fid == file_id) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                try self.compaction_queue.append(self.allocator, file_id);
                std.debug.print("Queued file {d} for compaction (stale: {d})\n", .{ file_id, new_stale });
            }
        }
    }

    pub const Iterator = index_module.OffHeapHashTable.Iterator;

    pub fn iterator(self: *WarthogDB) Iterator {
        return self.index.iterator(self.allocator);
    }

    pub fn delete(self: *WarthogDB, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.index.get(key)) |old_entry| {
            const stale_size = record.RecordHeader.SIZE + key.len + old_entry.value_size;
            try self.markFileAsStale(old_entry.file_id, stale_size);
            _ = self.index.remove(key);
        } else {
            return;
        }

        if (self.current_tombstone_file == null) {
            if (self.current_write_file) |f| {
                self.current_tombstone_file = try log_file.TombstoneFile.open(self.dir, f.file_id, true);
            } else {
                return error.NotOpen;
            }
        }

        var entry = try tombstone_file.TombstoneEntry.init(key, 0, 0);
        _ = try self.current_tombstone_file.?.writeEntry(&entry);
    }

    fn getNextFileId(self: *WarthogDB) u32 {
        return @atomicRmw(u32, &self.next_file_id, .Add, 1, .monotonic);
    }

    fn runCompaction(self: *WarthogDB) void {
        while (self.metadata.open) {
            var file_to_compact: ?u32 = null;

            {
                self.mutex.lock();
                defer self.mutex.unlock();
                if (self.compaction_queue.items.len > 0) {
                    file_to_compact = self.compaction_queue.orderedRemove(0);
                }
            }

            if (file_to_compact) |fid| {
                self.compactFile(fid) catch |err| {
                    std.debug.print("Compaction failed for file {d}: {}\n", .{ fid, err });
                };
                std.Thread.sleep(100 * std.time.ns_per_ms);
            }
        }
    }

    fn compactFile(self: *WarthogDB, file_id: u32) !void {
        var file: log_file.LogFile = undefined;
        {
            self.mutex.lock();
            if (self.read_files.get(file_id)) |f| {
                file = f.*;
            } else {
                self.mutex.unlock();
                return;
            }
            self.mutex.unlock();
        }

        var idx_iter = try file.index_file.iterator(self.allocator);
        var records_copied: usize = 0;

        while (try idx_iter.next()) |idx_entry| {
            defer self.allocator.free(idx_entry.key);

            var fresh = false;
            var current_val_offset: u32 = 0;
            if (self.index.get(idx_entry.key)) |current_meta| {
                const val_offset = idx_entry.header.record_offset + record.RecordHeader.SIZE + idx_entry.key.len;

                if (current_meta.file_id == file_id and current_meta.value_offset == val_offset) {
                    fresh = true;
                    current_val_offset = @intCast(val_offset);
                }
            }

            if (fresh) {
                try self.copyRecordToCompactionFile(file, idx_entry, current_val_offset);
                records_copied += 1;
            }
        }

        if (records_copied > 0) {
            if (self.compaction_write_file) |_| {}
        }

        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.read_files.fetchRemove(file_id)) |kv| {
                var f = kv.value;
                f.marked_for_delete = true;
                f.decref();
            }
            _ = self.stale_data_map.remove(file_id);
        }
    }

    fn copyRecordToCompactionFile(self: *WarthogDB, from_file: log_file.LogFile, idx_entry: index_file.IndexFileEntry, old_val_offset: u32) !void {
        if (self.compaction_write_file == null) {
            const new_id = self.getNextFileId();
            const write_file = try log_file.LogFile.open(self.dir, new_id, true);
            const wf_ptr = try self.allocator.create(log_file.LogFile);
            wf_ptr.* = write_file;
            self.compaction_write_file = wf_ptr;
        }

        const record_offset = idx_entry.header.record_offset;
        const total_size = idx_entry.header.record_size;

        const buf = try self.allocator.alloc(u8, total_size);
        defer self.allocator.free(buf);

        {
            const bytes_read = try from_file.file.preadAll(buf, record_offset);
            if (bytes_read != total_size) return error.UnexpectedEOF;
        }

        var target = self.compaction_write_file.?;
        _ = try target.file.writeAll(buf);
        const new_offset = target.write_offset;

        target.write_offset += total_size;

        var disk_idx_entry = try index_file.IndexFileEntry.init(idx_entry.key, idx_entry.header.record_size, @intCast(new_offset), idx_entry.header.sequence_number, idx_entry.header.version);
        try target.index_file.write(&disk_idx_entry);

        const val_size = idx_entry.header.record_size - @as(u32, @intCast(record.RecordHeader.SIZE + idx_entry.key.len));

        const new_idx_entry = index_module.IndexEntry{
            .file_id = target.file_id,
            .value_offset = @intCast(new_offset + record.RecordHeader.SIZE + idx_entry.key.len),
            .value_size = val_size,
            .sequence_number = idx_entry.header.sequence_number,
        };

        const old_index_entry = index_module.IndexEntry{
            .file_id = from_file.file_id,
            .value_offset = old_val_offset,
            .value_size = val_size,
            .sequence_number = idx_entry.header.sequence_number,
        };

        if (!self.index.replace(idx_entry.key, old_index_entry, new_idx_entry)) {
            try self.markFileAsStale(new_idx_entry.file_id, total_size);
        }
    }
};

test "WarthogDB basic put get" {
    const allocator = std.testing.allocator;
    const test_dir = "test_db_basic";
    std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var db = try WarthogDB.open(allocator, test_dir, .{});
    defer db.close();

    try db.put("hello", "world");

    const val = try db.get("hello", allocator);
    try std.testing.expect(val != null);
    try std.testing.expect(std.mem.eql(u8, val.?, "world"));
    allocator.free(val.?);
}

test "WarthogDB recovery" {
    const allocator = std.testing.allocator;
    const test_dir = "test_db_recovery";
    std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    {
        var db = try WarthogDB.open(allocator, test_dir, .{});
        defer db.close();
        try db.put("k1", "v1");
        try db.put("k2", "v2");
    }

    {
        var db = try WarthogDB.open(allocator, test_dir, .{});
        defer db.close();

        const v1 = try db.get("k1", allocator);
        try std.testing.expect(v1 != null);
        try std.testing.expect(std.mem.eql(u8, v1.?, "v1"));
        allocator.free(v1.?);

        const v2 = try db.get("k2", allocator);
        try std.testing.expect(v2 != null);
        try std.testing.expect(std.mem.eql(u8, v2.?, "v2"));
        allocator.free(v2.?);

        try db.put("k3", "v3");
        const v3 = try db.get("k3", allocator);
        try std.testing.expect(v3 != null);
        allocator.free(v3.?);
    }
}

test "WarthogDB delete and recovery" {
    const allocator = std.testing.allocator;
    const test_dir = "test_db_delete";
    std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    {
        var db = try WarthogDB.open(allocator, test_dir, .{});
        defer db.close();
        try db.put("k1", "v1");
        try db.put("k2", "v2");
        try db.delete("k1");
    }

    {
        var db = try WarthogDB.open(allocator, test_dir, .{});
        defer db.close();

        const v1 = try db.get("k1", allocator);
        try std.testing.expect(v1 == null);

        const v2 = try db.get("k2", allocator);
        try std.testing.expect(v2 != null);
        try std.testing.expect(std.mem.eql(u8, v2.?, "v2"));
        allocator.free(v2.?);
    }
}

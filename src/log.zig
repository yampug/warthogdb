const std = @import("std");
const record = @import("record.zig");
const index_file = @import("index_file.zig");
const Allocator = std.mem.Allocator;

const posix = if (@hasDecl(std, "posix")) std.posix else std.os;

pub const LogFile = struct {
    file: std.fs.File,
    file_id: u32,
    write_offset: u64,
    index_file: index_file.IndexFile,
    ref_count: usize,
    marked_for_delete: bool,
    full_path: []u8,
    dir: std.fs.Dir,

    mem_map: ?[]align(16384) u8,

    // Write Buffer
    write_buffer: []u8,
    buffer_offset: usize,
    buffer_capacity: usize = 64 * 1024,

    pub fn open(base_dir: std.fs.Dir, file_id: u32, create: bool) !LogFile {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "{d}.data", .{file_id});
        errdefer std.heap.page_allocator.free(name);

        const file = if (create)
            try base_dir.createFile(name, .{ .read = true, .truncate = false })
        else
            try base_dir.openFile(name, .{});

        // Open associated index file
        const idx_file = try index_file.IndexFile.open(base_dir, file_id, create);

        // Duplicate dir handle for independent lifecycle
        const dir = try base_dir.openDir(".", .{});

        // seek to end if writing
        const stat = try file.stat();
        if (create) {
            try file.seekTo(stat.size);
        }

        // Try to mmap if reading existing file
        var map: ?[]align(16384) u8 = null;
        if (!create) {
            const size = stat.size;
            if (size > 0) {
                // MAP_SHARED for page cache sharing
                if (posix.mmap(null, size, posix.PROT.READ, .{ .TYPE = .SHARED }, file.handle, 0)) |m| {
                    map = m;
                } else |err| {
                    std.debug.print("Failed to mmap file {d}: {}\n", .{ file_id, err });
                }
            }
        }

        const write_buffer = try std.heap.page_allocator.alloc(u8, 64 * 1024);

        return .{
            .file = file,
            .file_id = file_id,
            .write_offset = stat.size,
            .index_file = idx_file,
            .ref_count = 1, // Owned by DB initially
            .marked_for_delete = false,
            .full_path = name,
            .dir = dir,
            .mem_map = map,
            .write_buffer = write_buffer,
            .buffer_offset = 0,
        };
    }

    pub fn close(self: *LogFile) void {
        self.flush() catch |err| {
            std.debug.print("Failed to flush log file {d} on close: {}\n", .{ self.file_id, err });
        };

        if (self.mem_map) |m| {
            posix.munmap(m);
            self.mem_map = null;
        }
        const allocator = std.heap.page_allocator;
        allocator.free(self.write_buffer);

        self.index_file.close();
        self.file.close();
        self.dir.close();
        allocator.free(self.full_path);
    }

    pub fn flush(self: *LogFile) !void {
        if (self.buffer_offset > 0) {
            // Seek to ensure we write at the correct physical location
            // write_offset tracks the LOGICAL end (including buffered data).
            // Physical write MUST happen at write_offset - buffer_offset.
            try self.file.seekTo(self.write_offset - self.buffer_offset);
            try self.file.writeAll(self.write_buffer[0..self.buffer_offset]);
            self.buffer_offset = 0;
        }
    }

    pub fn incref(self: *LogFile) void {
        _ = @atomicRmw(usize, &self.ref_count, .Add, 1, .seq_cst);
    }

    pub fn decref(self: *LogFile) void {
        const prev = @atomicRmw(usize, &self.ref_count, .Sub, 1, .seq_cst);
        if (prev == 1) {
            // Last ref dropped.
            if (self.marked_for_delete) {
                self.deletePhysical() catch |err| {
                    std.debug.print("Failed to delete physical file {d}: {}\n", .{ self.file_id, err });
                };
            } else {
                self.close();
            }
        }
    }

    fn deletePhysical(self: *LogFile) !void {
        if (self.mem_map) |m| {
            posix.munmap(m);
            self.mem_map = null;
        }
        self.index_file.close();
        self.file.close();

        // Delete files using stored dir handle
        self.dir.deleteFile(self.full_path) catch {};

        const name_idx = std.fmt.allocPrint(std.heap.page_allocator, "{d}.index", .{self.file_id}) catch return;
        defer std.heap.page_allocator.free(name_idx);
        self.dir.deleteFile(name_idx) catch {};

        self.dir.close();
        std.heap.page_allocator.free(self.full_path);
    }

    pub fn enableMmap(self: *LogFile) !void {
        if (self.mem_map != null) return;
        const stat = try self.file.stat();
        if (stat.size > 0) {
            self.mem_map = try posix.mmap(null, stat.size, posix.PROT.READ, .{ .TYPE = .SHARED }, self.file.handle, 0);
        }
    }

    pub fn writeRecord(self: *LogFile, rec: *record.Record) !u64 {
        rec.header.check_sum = rec.computeChecksum();
        const current_logical_offset = self.write_offset;

        // Calculate size including header
        const total_size = record.RecordHeader.SIZE + rec.key.len + rec.value.len;

        // If fits in buffer, copy
        if (self.buffer_offset + total_size <= self.buffer_capacity) {
            rec.header.serialize(self.write_buffer[self.buffer_offset..][0..record.RecordHeader.SIZE]);
            const k_start = self.buffer_offset + record.RecordHeader.SIZE;
            @memcpy(self.write_buffer[k_start..][0..rec.key.len], rec.key);
            const v_start = k_start + rec.key.len;
            @memcpy(self.write_buffer[v_start..][0..rec.value.len], rec.value);

            self.buffer_offset += total_size;
            self.write_offset += total_size;
            return current_logical_offset;
        }

        // Doesn't fit. Flush buffer first.
        try self.flush();

        // If still too big for empty buffer, write direct
        if (total_size > self.buffer_capacity) {
            var header_buf: [record.RecordHeader.SIZE]u8 = undefined;
            rec.header.serialize(&header_buf);
            try self.file.seekTo(self.write_offset);
            try self.file.writeAll(&header_buf);
            try self.file.writeAll(rec.key);
            try self.file.writeAll(rec.value);
            self.write_offset += total_size;
            return current_logical_offset;
        }

        // Otherwise, write to empty buffer
        rec.header.serialize(self.write_buffer[self.buffer_offset..][0..record.RecordHeader.SIZE]);
        const k_start = self.buffer_offset + record.RecordHeader.SIZE;
        @memcpy(self.write_buffer[k_start..][0..rec.key.len], rec.key);
        const v_start = k_start + rec.key.len;
        @memcpy(self.write_buffer[v_start..][0..rec.value.len], rec.value);

        self.buffer_offset += total_size;
        self.write_offset += total_size;
        return current_logical_offset;
    }

    pub fn readRecord(self: *LogFile, offset: u64, allocator: Allocator) !record.Record {
        // Read header
        var header_buf: [record.RecordHeader.SIZE]u8 = undefined;
        const bytes_read = try self.file.preadAll(&header_buf, offset);
        if (bytes_read != record.RecordHeader.SIZE) return error.UnexpectedEOF;

        const header = record.RecordHeader.deserialize(&header_buf);

        // Read key and value
        const total_len = header.key_size + header.value_size;
        const kv_buf = try allocator.alloc(u8, total_len);
        defer allocator.free(kv_buf);

        const kv_read = try self.file.preadAll(kv_buf, offset + record.RecordHeader.SIZE);
        if (kv_read != total_len) return error.UnexpectedEOF;

        const key = try allocator.dupe(u8, kv_buf[0..header.key_size]);
        const value = try allocator.dupe(u8, kv_buf[header.key_size..]);

        return record.Record{
            .header = header,
            .key = key,
            .value = value,
        };
    }

    pub fn readRecordView(self: *LogFile, offset: u64, allocator: Allocator) !record.RecordView {
        // 0. Check Write Buffer (Read-After-Write Consistency)
        if (self.buffer_offset > 0) {
            const buffer_start_offset = self.write_offset - self.buffer_offset;

            if (offset >= buffer_start_offset) {
                // Data is in the buffer
                const local_offset = @as(usize, @intCast(offset - buffer_start_offset));
                if (local_offset + record.RecordHeader.SIZE <= self.buffer_offset) {
                    const header_slice = self.write_buffer[local_offset..][0..record.RecordHeader.SIZE];
                    const header = record.RecordHeader.deserialize(header_slice);
                    const total_len = header.key_size + header.value_size;
                    const kv_start = local_offset + record.RecordHeader.SIZE;

                    if (kv_start + total_len <= self.buffer_offset) {
                        // DANGER: Buffer is mutable and can be flushed!
                        // We MUST copy if we return a view that outlives the buffer flush?
                        // RecordView has owning semantics if backing_buffer is set.
                        const buffer = try allocator.alloc(u8, total_len);
                        @memcpy(buffer, self.write_buffer[kv_start..][0..total_len]);

                        return record.RecordView{
                            .header = header,
                            .key = buffer[0..header.key_size],
                            .value = buffer[header.key_size..],
                            .backing_buffer = buffer,
                            .allocator = allocator,
                        };
                    }
                }
            }
        }

        // Optimistic: Check mmap
        if (self.mem_map) |map| {
            if (offset + record.RecordHeader.SIZE <= map.len) {
                const header_slice = map[offset .. offset + record.RecordHeader.SIZE];
                const header = record.RecordHeader.deserialize(header_slice);
                const kv_offset = offset + record.RecordHeader.SIZE;
                const total_len = header.key_size + header.value_size;
                if (kv_offset + total_len <= map.len) {
                    return record.RecordView{
                        .header = header,
                        .key = map[kv_offset .. kv_offset + header.key_size],
                        .value = map[kv_offset + header.key_size .. kv_offset + total_len],
                        .backing_buffer = null,
                        .allocator = null,
                    };
                }
            }
        }

        // Fallback: Single-ish Allocation
        // Allocate ONE buffer for Header + Key + Value
        var header_buf: [record.RecordHeader.SIZE]u8 = undefined;
        // Could be mmap failed or offset out of bounds (growing file not remapped). Fallback to pread.
        const bytes_read = try self.file.preadAll(&header_buf, offset);
        if (bytes_read != record.RecordHeader.SIZE) return error.UnexpectedEOF;

        const header = record.RecordHeader.deserialize(&header_buf);
        const total_len = header.key_size + header.value_size;

        const buffer = try allocator.alloc(u8, total_len);
        errdefer allocator.free(buffer);

        const kv_read = try self.file.preadAll(buffer, offset + record.RecordHeader.SIZE);
        if (kv_read != total_len) return error.UnexpectedEOF;

        return record.RecordView{
            .header = header,
            .key = buffer[0..header.key_size],
            .value = buffer[header.key_size..],
            .backing_buffer = buffer,
            .allocator = allocator,
        };
    }

    pub const Iterator = struct {
        file: *LogFile,
        current_offset: u64,
        allocator: Allocator,

        pub fn next(self: *Iterator) !?record.Record {
            var header_buf: [record.RecordHeader.SIZE]u8 = undefined;
            const bytes_read = try self.file.file.preadAll(&header_buf, self.current_offset);
            if (bytes_read == 0) return null; // EOF
            if (bytes_read != record.RecordHeader.SIZE) return error.UnexpectedEOF;

            const header = record.RecordHeader.deserialize(&header_buf);
            if (header.key_size == 0 or header.value_size == 0) {
                // Corrupt or empty?
            }

            const total_len = header.key_size + header.value_size;
            const kv_buf = try self.allocator.alloc(u8, total_len);
            defer self.allocator.free(kv_buf);

            const kv_read = try self.file.file.preadAll(kv_buf, self.current_offset + record.RecordHeader.SIZE);
            if (kv_read != total_len) return error.UnexpectedEOF;

            const key = try self.allocator.dupe(u8, kv_buf[0..header.key_size]);
            const value = try self.allocator.dupe(u8, kv_buf[header.key_size..]);

            self.current_offset += record.RecordHeader.SIZE + total_len;

            return record.Record{
                .header = header,
                .key = key,
                .value = value,
            };
        }
    };

    pub fn iterator(self: *LogFile, allocator: Allocator) Iterator {
        return Iterator{
            .file = self,
            .current_offset = 0,
            .allocator = allocator,
        };
    }
};

pub const TombstoneFile = struct {
    file: std.fs.File,
    file_id: u32,

    pub fn open(dir: std.fs.Dir, file_id: u32, create: bool) !TombstoneFile {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "{d}.tombstone", .{file_id});
        defer std.heap.page_allocator.free(name);

        const file = if (create)
            try dir.createFile(name, .{ .read = true, .truncate = false })
        else
            try dir.openFile(name, .{});

        return TombstoneFile{
            .file = file,
            .file_id = file_id,
        };
    }

    pub fn close(self: *TombstoneFile) void {
        self.file.close();
    }

    pub fn writeEntry(self: *TombstoneFile, entry: *const @import("tombstone.zig").TombstoneEntry) !u64 {
        // Tombstone is header + key.
        const tombstone = @import("tombstone.zig");
        var header_buf: [tombstone.TombstoneHeader.SIZE]u8 = undefined;
        entry.header.serialize(&header_buf);

        try self.file.writeAll(&header_buf);
        try self.file.writeAll(entry.key);

        return 0; // offset?
    }

    pub fn readEntry(self: *TombstoneFile, offset: u64, allocator: Allocator) !@import("tombstone.zig").TombstoneEntry {
        const tombstone = @import("tombstone.zig");
        var header_buf: [tombstone.TombstoneHeader.SIZE]u8 = undefined;
        const bytes_read = try self.file.preadAll(&header_buf, offset);
        if (bytes_read != tombstone.TombstoneHeader.SIZE) return error.UnexpectedEOF;

        const header = tombstone.TombstoneHeader.deserialize(&header_buf);
        const key_buf = try allocator.alloc(u8, header.key_size);
        const read_key = try self.file.preadAll(key_buf, offset + tombstone.TombstoneHeader.SIZE);
        if (read_key != header.key_size) return error.UnexpectedEOF;

        return tombstone.TombstoneEntry{
            .header = header,
            .key = key_buf,
        };
    }
};

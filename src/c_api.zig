const std = @import("std");
const warthog = @import("warthogdb.zig");

// Global allocator for the C API integration
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();

pub const WarthogHandle = *warthog.WarthogDB;
pub const SnapshotHandle = *warthog.snapshot.Snapshot;

export fn warthog_open(path: [*c]const u8, max_file_size: u32, compaction_threshold: f64, number_of_records: u32) ?WarthogHandle {
    const path_slice = std.mem.span(path);

    var options = warthog.WarthogDBOptions{
        .fixed_key_size = 0, // variable
        .number_of_records = number_of_records,
    };
    if (max_file_size > 0) options.max_file_size = max_file_size;
    if (compaction_threshold > 0) options.compaction_threshold_per_file = compaction_threshold;

    const db = allocator.create(warthog.WarthogDB) catch return null;
    if (warthog.WarthogDB.open(allocator, path_slice, options)) |opened_db| {
        db.* = opened_db;
        db.startCompaction() catch {
            db.deinit();
            allocator.destroy(db);
            return null;
        };
    } else |_| {
        allocator.destroy(db);
        return null;
    }

    return db;
}

export fn warthog_close(handle: ?WarthogHandle) void {
    if (handle) |db| {
        db.close();
        allocator.destroy(db);
    }
}

export fn warthog_put(handle: ?WarthogHandle, key: [*c]const u8, key_len: usize, val: [*c]const u8, val_len: usize) i32 {
    if (handle) |db| {
        const key_slice = key[0..key_len];
        const val_slice = val[0..val_len];
        db.put(key_slice, val_slice) catch |err| {
            std.debug.print("Put error: {}\n", .{err});
            return -1;
        };
        return 0;
    }
    return -1;
}

export fn warthog_get(handle: ?WarthogHandle, key: [*c]const u8, key_len: usize, out_val: [*c]u8, out_cap: usize, out_len: *usize) i32 {
    if (handle) |db| {
        const key_slice = key[0..key_len];
        const result_opt = db.get(key_slice, allocator) catch |err| {
            if (err == error.FileNotFound) return 1;
            std.debug.print("Get error: {}\n", .{err});
            return -1;
        };

        if (result_opt) |res| {
            defer allocator.free(res);
            out_len.* = res.len;
            if (res.len > out_cap) {
                return 2;
            }
            @memcpy(out_val[0..res.len], res);
            return 0;
        } else {
            return 1;
        }
    }
    return -1;
}

export fn warthog_snapshot_open(handle: ?WarthogHandle) ?SnapshotHandle {
    if (handle) |db| {
        return db.openSnapshot() catch return null;
    }
    return null;
}

export fn warthog_snapshot_close(handle: ?WarthogHandle, snap: ?SnapshotHandle) void {
    if (handle) |db| {
        if (snap) |s| {
            db.closeSnapshot(s);
        }
    }
}

export fn warthog_snapshot_get(handle: ?WarthogHandle, snap: ?SnapshotHandle, key: [*c]const u8, key_len: usize, out_val: [*c]u8, out_cap: usize, out_len: *usize) i32 {
    if (handle) |db| {
        if (snap) |s| {
            const key_slice = key[0..key_len];
            const result_opt = db.getFromSnapshot(s, key_slice, allocator) catch |err| {
                if (err == error.FileNotFound) return 1;
                std.debug.print("Snapshot Get error: {}\n", .{err});
                return -1;
            };

            if (result_opt) |res| {
                defer allocator.free(res);
                out_len.* = res.len;
                if (res.len > out_cap) {
                    return 2;
                }
                @memcpy(out_val[0..res.len], res);
                return 0;
            } else {
                return 1;
            }
        }
    }
    return -1;
}

export fn warthog_delete(handle: ?WarthogHandle, key: [*c]const u8, key_len: usize) i32 {
    if (handle) |db| {
        const key_slice = key[0..key_len];
        db.delete(key_slice) catch |err| {
            std.debug.print("Delete error: {}\n", .{err});
            return -1;
        };
        return 0;
    }
    return -1;
}

const WarthogIterator = struct {
    iter: warthog.WarthogDB.Iterator,
    db: *warthog.WarthogDB,
    snap: ?SnapshotHandle = null,
};

export fn warthog_iter_open(handle: ?WarthogHandle) ?*WarthogIterator {
    if (handle) |db| {
        const iter_wrapper = allocator.create(WarthogIterator) catch return null;
        iter_wrapper.iter = db.iterator();
        iter_wrapper.db = db;
        iter_wrapper.snap = null;
        return iter_wrapper;
    }
    return null;
}

export fn warthog_snapshot_iter_open(handle: ?WarthogHandle, snap: ?SnapshotHandle) ?*WarthogIterator {
    if (handle) |db| {
        const iter_wrapper = allocator.create(WarthogIterator) catch return null;
        iter_wrapper.iter = db.iterator();
        iter_wrapper.db = db;
        iter_wrapper.snap = snap;
        return iter_wrapper;
    }
    return null;
}

export fn warthog_iter_next(iter_ptr: ?*WarthogIterator, key_out: [*c]u8, key_cap: usize, key_len: *usize, val_out: [*c]u8, val_cap: usize, val_len: *usize) i32 {
    if (iter_ptr) |wrapper| {
        while (true) {
            const result = wrapper.iter.next() catch return -1;
            if (result) |entry| {
                var val_opt: ?[]u8 = null;

                if (wrapper.snap) |s| {
                    // Try reading from file offset contained in entry
                    // entry.value is IndexEntry, which has file_id and value_offset.
                    // IMPORTANT: Verify field names of entry!
                    // The compiler will error if fields don't exist.

                    if (s.getFile(entry.value.file_id)) |f| {
                        const record_header_size = 33;
                        const record_start = entry.value.value_offset - entry.key.len - record_header_size;

                        const rec = f.readRecord(record_start, allocator) catch continue;

                        if (!std.mem.eql(u8, rec.key, entry.key)) {
                            allocator.free(rec.key);
                            allocator.free(rec.value);
                            continue;
                        }

                        allocator.free(rec.key);
                        val_opt = @constCast(rec.value);
                    } else {
                        continue;
                    }
                } else {
                    val_opt = wrapper.db.get(entry.key, allocator) catch return -1;
                }

                if (val_opt) |val| {
                    defer allocator.free(val);

                    if (entry.key.len > key_cap or val.len > val_cap) {
                        key_len.* = entry.key.len;
                        val_len.* = val.len;
                        return 2;
                    }

                    key_len.* = entry.key.len;
                    val_len.* = val.len;

                    @memcpy(key_out[0..entry.key.len], entry.key);
                    @memcpy(val_out[0..val.len], val);
                    return 0;
                } else {
                    continue;
                }
            } else {
                return 1;
            }
        }
    }
    return -1;
}

export fn warthog_iter_close(iter_ptr: ?*WarthogIterator) void {
    if (iter_ptr) |wrapper| {
        allocator.destroy(wrapper);
    }
}

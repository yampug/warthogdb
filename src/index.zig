const std = @import("std");
const hash = @import("hash.zig");
const mem = @import("mem.zig");
const Allocator = std.mem.Allocator;
const RwLock = std.Thread.RwLock;
const Mutex = std.Thread.Mutex;

/// Value stored in the in-memory index.
/// Represents the location of a record on disk.
pub const IndexEntry = struct {
    file_id: u32,
    value_offset: u32,
    value_size: u32,
    sequence_number: u64,
    // Add other fields as needed (tombstone bit, etc.)
};

/// A concurrent hash table partitioned into segments.
pub const OffHeapHashTable = struct {
    segments: []Segment,
    allocator: Allocator,
    hasher: hash.Murmur3, // Or generic hasher

    pub fn init(allocator: Allocator, segment_count: usize) !OffHeapHashTable {
        // Round up segment count to power of 2
        var real_segment_count: usize = 1;
        while (real_segment_count < segment_count) {
            real_segment_count <<= 1;
        }

        const segments_slice = try allocator.alloc(Segment, real_segment_count);
        for (segments_slice) |*seg| {
            seg.* = Segment.init(allocator);
        }

        return .{
            .segments = segments_slice,
            .allocator = allocator,
            .hasher = .{},
        };
    }

    pub fn deinit(self: *OffHeapHashTable) void {
        for (self.segments) |*seg| {
            seg.deinit();
        }
        self.allocator.free(self.segments);
    }

    pub fn put(self: *OffHeapHashTable, key: []const u8, value: IndexEntry) !void {
        const h = std.hash.Wyhash.hash(0, key);
        const seg_idx = h & (self.segments.len - 1);
        var seg = &self.segments[seg_idx];

        seg.mutex.lock();
        defer seg.mutex.unlock();

        try seg.put(key, value);
    }

    pub fn get(self: *OffHeapHashTable, key: []const u8) ?IndexEntry {
        const h = std.hash.Wyhash.hash(0, key);
        const seg_idx = h & (self.segments.len - 1);
        var seg = &self.segments[seg_idx];

        seg.mutex.lock();
        defer seg.mutex.unlock();

        return seg.get(key);
    }

    pub fn remove(self: *OffHeapHashTable, key: []const u8) bool {
        const h = std.hash.Wyhash.hash(0, key);
        const seg_idx = h & (self.segments.len - 1);
        var seg = &self.segments[seg_idx];

        seg.mutex.lock();
        defer seg.mutex.unlock();

        return seg.remove(key);
    }

    pub fn replace(self: *OffHeapHashTable, key: []const u8, old_val: IndexEntry, new_val: IndexEntry) bool {
        const h = std.hash.Wyhash.hash(0, key);
        const seg_idx = h & (self.segments.len - 1);
        var seg = &self.segments[seg_idx];

        seg.mutex.lock();
        defer seg.mutex.unlock();

        return seg.replace(key, old_val, new_val);
    }

    pub fn iterator(self: *OffHeapHashTable, allocator: Allocator) Iterator {
        return Iterator.init(self, allocator);
    }

    pub const Iterator = struct {
        ht: *OffHeapHashTable,
        allocator: Allocator,
        current_segment_idx: usize,
        snapshot: std.ArrayListUnmanaged(SnapshotEntry),
        snapshot_idx: usize,

        pub const SnapshotEntry = struct {
            key: []const u8,
            value: IndexEntry,
        };

        pub fn init(ht: *OffHeapHashTable, allocator: Allocator) Iterator {
            return .{
                .ht = ht,
                .allocator = allocator,
                .current_segment_idx = 0,
                .snapshot = .{},
                .snapshot_idx = 0,
            };
        }

        pub fn deinit(self: *Iterator) void {
            self.clearSnapshot();
            self.snapshot.deinit(self.allocator);
        }

        fn clearSnapshot(self: *Iterator) void {
            for (self.snapshot.items) |entry| {
                self.allocator.free(entry.key);
            }
            self.snapshot.clearRetainingCapacity();
            self.snapshot_idx = 0;
        }

        pub fn next(self: *Iterator) !?SnapshotEntry {
            while (true) {
                if (self.snapshot_idx < self.snapshot.items.len) {
                    const entry = self.snapshot.items[self.snapshot_idx];
                    self.snapshot_idx += 1;
                    return entry;
                }

                if (self.current_segment_idx >= self.ht.segments.len) {
                    return null;
                }

                // Clear previous and prep for new segment
                self.clearSnapshot();

                var seg = &self.ht.segments[self.current_segment_idx];
                self.current_segment_idx += 1;

                seg.mutex.lockShared();
                defer seg.mutex.unlockShared();

                var it = seg.map.iterator();
                while (it.next()) |entry| {
                    const key_dup = try self.allocator.dupe(u8, entry.key_ptr.*);
                    errdefer self.allocator.free(key_dup);
                    try self.snapshot.append(self.allocator, .{ .key = key_dup, .value = entry.value_ptr.* });
                }
            }
        }
    };
};

const Segment = struct {
    map: std.StringHashMap(IndexEntry),
    mutex: RwLock,
    allocator: Allocator,

    pub fn init(allocator: Allocator) Segment {
        return .{
            .map = std.StringHashMap(IndexEntry).init(allocator),
            .mutex = RwLock{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Segment) void {
        var it = self.map.keyIterator();
        while (it.next()) |key_ptr| {
            self.allocator.free(key_ptr.*);
        }
        self.map.deinit();
    }

    pub fn put(self: *Segment, key: []const u8, value: IndexEntry) !void {
        // Check if updating existing key to reuse memory
        if (self.map.getEntry(key)) |existing_entry| {
            // Key content is same, just update value.
            // We can reuse the stored key pointer since it's identical string content.
            existing_entry.value_ptr.* = value;
            return;
        }

        // New key, duplicate it
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        try self.map.put(key_copy, value);
    }

    pub fn get(self: *Segment, key: []const u8) ?IndexEntry {
        return self.map.get(key);
    }

    pub fn remove(self: *Segment, key: []const u8) bool {
        if (self.map.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            return true;
        }
        return false;
    }

    pub fn replace(self: *Segment, key: []const u8, old_val: IndexEntry, new_val: IndexEntry) bool {
        if (self.map.getEntry(key)) |entry| {
            // Check identity/equality of current value with old_val
            if (std.meta.eql(entry.value_ptr.*, old_val)) {
                entry.value_ptr.* = new_val;
                return true;
            }
        }
        return false;
    }
};

test "OffHeapHashTable basic usage" {
    const allocator = std.testing.allocator;
    var ht = try OffHeapHashTable.init(allocator, 4);
    defer ht.deinit();

    const entry = IndexEntry{
        .file_id = 1,
        .value_offset = 100,
        .value_size = 50,
        .sequence_number = 1,
    };

    try ht.put("key1", entry);

    if (ht.get("key1")) |got| {
        try std.testing.expectEqual(got.file_id, 1);
        try std.testing.expectEqual(got.value_offset, 100);
    } else {
        return error.NotFound;
    }

    try std.testing.expect(ht.get("key2") == null);
}

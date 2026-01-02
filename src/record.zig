const std = @import("std");
const hash = @import("hash.zig");
const Allocator = std.mem.Allocator;

pub const RecordHeader = struct {
    pub const SIZE: usize = 18;
    pub const CHECKSUM_SIZE: usize = 4;
    pub const SEQUENCE_NUMBER_OFFSET: usize = 10;

    check_sum: u32,
    version: u8,
    key_size: u8,
    value_size: u32,
    sequence_number: u64,

    pub fn serialize(self: RecordHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= SIZE);
        std.mem.writeInt(u32, buffer[0..4], self.check_sum, .big);
        buffer[4] = self.version;
        buffer[5] = self.key_size;
        std.mem.writeInt(u32, buffer[6..10], self.value_size, .big);
        std.mem.writeInt(u64, buffer[10..18], self.sequence_number, .big);
    }

    pub fn deserialize(buffer: []const u8) RecordHeader {
        std.debug.assert(buffer.len >= SIZE);
        return .{
            .check_sum = std.mem.readInt(u32, buffer[0..4], .big),
            .version = buffer[4],
            .key_size = buffer[5],
            .value_size = std.mem.readInt(u32, buffer[6..10], .big),
            .sequence_number = std.mem.readInt(u64, buffer[10..18], .big),
        };
    }
};

pub const Record = struct {
    header: RecordHeader,
    key: []const u8,
    value: []const u8,

    pub fn init(key: []const u8, value: []const u8, seq: u64, version: u8) !Record {
        if (key.len > 255) return error.KeyTooLarge;
        if (value.len > std.math.maxInt(u32)) return error.ValueTooLarge;

        return .{
            .header = .{
                .check_sum = 0, // Computed later
                .version = version,
                .key_size = @intCast(key.len),
                .value_size = @intCast(value.len),
                .sequence_number = seq,
            },
            .key = key,
            .value = value,
        };
    }

    pub fn computeChecksum(self: *Record) u32 {
        // Java: crc32.update(header, Header.CHECKSUM_OFFSET + Header.CHECKSUM_SIZE, ...);
        // meaning skip first 4 bytes.
        var buf: [RecordHeader.SIZE]u8 = undefined;
        self.header.serialize(&buf);

        // Use CRC32 hash of header[4..] + key + value
        // Note: Hash logic must match Java's CRC32 exactly.
        // Zigs std.hash.Crc32 is slice-based.
        // Can update incrementally if implementing incremental checksum,
        // for now let's use a temporary buffer
        // std.hash.Crc32 allows single call.
        // To do incremental, we need a standard Crc32 struct.
        // Zig 0.13 std.hash.Crc32.init() -> Digest

        var crc = std.hash.Crc32.init();
        crc.update(buf[4..]); // Skip checksum field
        crc.update(self.key);
        crc.update(self.value);
        return crc.final();
    }
};

pub const RecordView = struct {
    header: RecordHeader,
    key: []const u8,
    value: []const u8,
    // Optional owning buffer. If null, slices are views into mmap or external memory.
    backing_buffer: ?[]u8 = null,
    allocator: ?Allocator = null,

    pub fn deinit(self: RecordView) void {
        if (self.backing_buffer) |buf| {
            if (self.allocator) |alloc| {
                alloc.free(buf);
            }
        }
    }
};

test "Record serialization" {
    const key = "key";
    const value = "value";
    var rec = try Record.init(key, value, 12345, 1);
    const checksum = rec.computeChecksum();
    rec.header.check_sum = checksum;

    var buf: [RecordHeader.SIZE]u8 = undefined;
    rec.header.serialize(&buf);

    const decoded = RecordHeader.deserialize(&buf);
    try std.testing.expectEqual(decoded.sequence_number, 12345);
    try std.testing.expectEqual(decoded.version, 1);
    try std.testing.expectEqual(decoded.key_size, key.len);
    try std.testing.expectEqual(decoded.value_size, value.len);
    try std.testing.expectEqual(decoded.check_sum, checksum);
}

const std = @import("std");
const hash = @import("hash.zig");

pub const TombstoneHeader = struct {
    pub const SIZE: usize = 14;
    pub const CHECKSUM_SIZE: usize = 4;
    pub const CHECKSUM_OFFSET: usize = 0;

    check_sum: u32,
    version: u8,
    key_size: u8,
    sequence_number: u64,

    pub fn serialize(self: TombstoneHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= SIZE);
        std.mem.writeInt(u32, buffer[0..4], self.check_sum, .big);
        buffer[4] = self.version;
        buffer[5] = self.key_size;
        std.mem.writeInt(u64, buffer[6..14], self.sequence_number, .big);
    }

    pub fn deserialize(buffer: []const u8) TombstoneHeader {
        std.debug.assert(buffer.len >= SIZE);
        return .{
            .check_sum = std.mem.readInt(u32, buffer[0..4], .big),
            .version = buffer[4],
            .key_size = buffer[5],
            .sequence_number = std.mem.readInt(u64, buffer[6..14], .big),
        };
    }
};

pub const TombstoneEntry = struct {
    header: TombstoneHeader,
    key: []const u8,

    pub fn init(key: []const u8, seq: u64, version: u8) !TombstoneEntry {
        if (key.len > 255) return error.KeyTooLarge;

        return .{
            .header = .{
                .check_sum = 0,
                .version = version,
                .key_size = @intCast(key.len),
                .sequence_number = seq,
            },
            .key = key,
        };
    }

    pub fn computeChecksum(self: *TombstoneEntry) u32 {
        var buf: [TombstoneHeader.SIZE]u8 = undefined;
        self.header.serialize(&buf);

        var crc = std.hash.Crc32.init();
        crc.update(buf[4..]); // Skip checksum
        crc.update(self.key);
        return crc.final();
    }
};

test "TombstoneEntry serialization" {
    const key = "tomb_key";
    var entry = try TombstoneEntry.init(key, 999, 1);
    const checksum = entry.computeChecksum();
    entry.header.check_sum = checksum;

    var buf: [TombstoneHeader.SIZE]u8 = undefined;
    entry.header.serialize(&buf);

    const decoded = TombstoneHeader.deserialize(&buf);
    try std.testing.expectEqual(decoded.sequence_number, 999);
    try std.testing.expectEqual(decoded.check_sum, checksum);
}

const std = @import("std");
const hash = @import("hash.zig");

pub const MetaData = struct {
    pub const SIZE: usize = 19;
    pub const CHECK_SUM_SIZE: usize = 4;
    pub const CHECK_SUM_OFFSET: usize = 0;
    pub const METADATA_FILE_NAME = "META";

    check_sum: u32,
    version: u8,
    open: bool,
    sequence_number: u64,
    io_error: bool,
    max_file_size: u32,

    pub fn init() MetaData {
        return .{
            .check_sum = 0,
            .version = 0,
            .open = true, // Default open on creation
            .sequence_number = 0,
            .io_error = false,
            .max_file_size = 1024 * 1024 * 1024, // Default 1GB
        };
    }

    pub fn serialize(self: *MetaData, buffer: []u8) void {
        std.debug.assert(buffer.len >= SIZE);

        // Serialize fields starting from offset 4 (after checksum)
        buffer[4] = self.version;
        buffer[5] = if (self.open) 0xFF else 0;
        std.mem.writeInt(u64, buffer[6..14], self.sequence_number, .big);
        buffer[14] = if (self.io_error) 0xFF else 0;
        std.mem.writeInt(u32, buffer[15..19], self.max_file_size, .big);

        // Compute Checksum
        // Java: crc32.update(header, CHECK_SUM_OFFSET + CHECK_SUM_SIZE, META_DATA_SIZE - CHECK_SUM_SIZE);
        // Validating range: 4..19
        var crc = std.hash.Crc32.init();
        crc.update(buffer[4..19]);
        self.check_sum = crc.final();

        // Write Checksum at offset 0
        std.mem.writeInt(u32, buffer[0..4], self.check_sum, .big);
    }

    pub fn deserialize(buffer: []const u8) MetaData {
        std.debug.assert(buffer.len >= SIZE);
        const check_sum = std.mem.readInt(u32, buffer[0..4], .big);
        const version = buffer[4];
        const open = buffer[5] != 0;
        const sequence_number = std.mem.readInt(u64, buffer[6..14], .big);
        const io_error = buffer[14] != 0;
        const max_file_size = std.mem.readInt(u32, buffer[15..19], .big);

        return .{
            .check_sum = check_sum,
            .version = version,
            .open = open,
            .sequence_number = sequence_number,
            .io_error = io_error,
            .max_file_size = max_file_size,
        };
    }
};

test "MetaData serialization" {
    var meta = MetaData.init();
    meta.sequence_number = 123456789;
    meta.open = false;
    meta.max_file_size = 2048;

    var buf: [MetaData.SIZE]u8 = undefined;
    meta.serialize(&buf);

    const decoded = MetaData.deserialize(&buf);

    try std.testing.expectEqual(decoded.sequence_number, meta.sequence_number);
    try std.testing.expectEqual(decoded.open, meta.open);
    try std.testing.expectEqual(decoded.max_file_size, meta.max_file_size);
    try std.testing.expectEqual(decoded.check_sum, meta.check_sum);
}

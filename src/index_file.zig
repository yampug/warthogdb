const std = @import("std");
const hash = @import("hash.zig");

pub const IndexFileHeader = struct {
    pub const SIZE: usize = 22;
    pub const CHECKSUM_SIZE: usize = 4;
    pub const CHECKSUM_OFFSET: usize = 0;

    check_sum: u32,
    version: u8,
    key_size: u8,
    record_size: u32,
    record_offset: u32,
    sequence_number: u64,

    pub fn serialize(self: IndexFileHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= SIZE);
        std.mem.writeInt(u32, buffer[0..4], self.check_sum, .big);
        buffer[4] = self.version;
        buffer[5] = self.key_size;
        std.mem.writeInt(u32, buffer[6..10], self.record_size, .big);
        std.mem.writeInt(u32, buffer[10..14], self.record_offset, .big);
        std.mem.writeInt(u64, buffer[14..22], self.sequence_number, .big);
    }

    pub fn deserialize(buffer: []const u8) IndexFileHeader {
        std.debug.assert(buffer.len >= SIZE);
        return .{
            .check_sum = std.mem.readInt(u32, buffer[0..4], .big),
            .version = buffer[4],
            .key_size = buffer[5],
            .record_size = std.mem.readInt(u32, buffer[6..10], .big),
            .record_offset = std.mem.readInt(u32, buffer[10..14], .big),
            .sequence_number = std.mem.readInt(u64, buffer[14..22], .big),
        };
    }
};

pub const IndexFileEntry = struct {
    header: IndexFileHeader,
    key: []const u8,

    pub fn init(key: []const u8, record_size: u32, record_offset: u32, seq: u64, version: u8) !IndexFileEntry {
        if (key.len > 255) return error.KeyTooLarge;

        return .{
            .header = .{
                .check_sum = 0,
                .version = version,
                .key_size = @intCast(key.len),
                .record_size = record_size,
                .record_offset = record_offset,
                .sequence_number = seq,
            },
            .key = key,
        };
    }

    pub fn computeChecksum(self: *IndexFileEntry) u32 {
        var buf: [IndexFileHeader.SIZE]u8 = undefined;
        self.header.serialize(&buf);

        var crc = std.hash.Crc32.init();
        crc.update(buf[4..]); // Skip checksum
        crc.update(self.key);
        return crc.final();
    }
};

test "IndexFileEntry serialization" {
    const key = "idx_key";
    var entry = try IndexFileEntry.init(key, 100, 200, 50, 0); // Version 0
    const checksum = entry.computeChecksum();
    entry.header.check_sum = checksum;

    var buf: [IndexFileHeader.SIZE]u8 = undefined;
    entry.header.serialize(&buf);

    const decoded = IndexFileHeader.deserialize(&buf);
    try std.testing.expectEqual(decoded.record_size, 100);
    try std.testing.expectEqual(decoded.check_sum, checksum);
}

pub const IndexFile = struct {
    file: std.fs.File,
    file_id: u32,
    write_offset: u64,

    pub fn open(dir: std.fs.Dir, file_id: u32, create: bool) !IndexFile {
        const name = try std.fmt.allocPrint(std.heap.page_allocator, "{d}.index", .{file_id});
        defer std.heap.page_allocator.free(name);

        const file = if (create)
            try dir.createFile(name, .{ .read = true, .truncate = false })
        else
            try dir.openFile(name, .{});

        const stat = try file.stat();
        if (create) {
            try file.seekTo(stat.size);
        }

        return .{
            .file = file,
            .file_id = file_id,
            .write_offset = stat.size,
        };
    }

    pub fn close(self: *IndexFile) void {
        self.file.close();
    }

    pub fn write(self: *IndexFile, entry: *IndexFileEntry) !void {
        entry.header.check_sum = entry.computeChecksum();

        var header_buf: [IndexFileHeader.SIZE]u8 = undefined;
        entry.header.serialize(&header_buf);

        _ = try self.file.write(&header_buf);
        _ = try self.file.write(entry.key);

        const written = IndexFileHeader.SIZE + entry.key.len;
        self.write_offset += written;
    }

    pub const Iterator = struct {
        file: std.fs.File,
        allocator: std.mem.Allocator,
        current_offset: u64 = 0,
        file_size: u64,

        pub fn next(self: *Iterator) !?IndexFileEntry {
            if (self.current_offset >= self.file_size) return null;

            // Seek to offset (technically not needed if we read sequentially, but safer)
            try self.file.seekTo(self.current_offset);

            // Read Header
            var header_buf: [IndexFileHeader.SIZE]u8 = undefined;
            const bytes_read = try self.file.readAll(&header_buf);
            if (bytes_read == 0) return null;
            if (bytes_read < IndexFileHeader.SIZE) return error.UnexpectedEOF;

            const header = IndexFileHeader.deserialize(&header_buf);

            // Read Key
            const key = try self.allocator.alloc(u8, header.key_size);
            // We transfer ownership of key to the returned Entry (caller frees)
            // But if we fail to read key, we must free.
            errdefer self.allocator.free(key);

            const key_read = try self.file.readAll(key);
            if (key_read != header.key_size) return error.UnexpectedEOF;

            self.current_offset += IndexFileHeader.SIZE + header.key_size;

            return IndexFileEntry{
                .header = header,
                .key = key,
            };
        }
    };

    pub fn iterator(self: *IndexFile, allocator: std.mem.Allocator) !Iterator {
        const stat = try self.file.stat();
        return Iterator{
            .file = self.file,
            .allocator = allocator,
            .file_size = stat.size,
        };
    }
};

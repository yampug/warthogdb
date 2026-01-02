const std = @import("std");

pub const hash = @import("hash.zig");
pub const mem = @import("mem.zig");
pub const index = @import("index.zig");
pub const record = @import("record.zig");
pub const log = @import("log.zig");
pub const index_file = @import("index_file.zig");
pub const tombstone = @import("tombstone.zig");
pub const metadata = @import("metadata.zig");
pub const warthogdb = @import("warthogdb.zig");
pub const snapshot = @import("snapshot.zig");

test {
    _ = hash;
    _ = mem;
    _ = index;
    _ = record;
    _ = log;
    _ = index_file;
    _ = tombstone;
    _ = warthogdb;
}

pub fn bufferedPrint() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try stdout.flush();
}

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    try std.testing.expect(add(3, 7) == 10);
}

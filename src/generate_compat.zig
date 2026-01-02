const std = @import("std");
const warthogdb_lib = @import("warthogdb");
const warthogdb = warthogdb_lib.warthogdb;
const Allocator = std.mem.Allocator;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const db_path = "../cross_verify_warthog_db";

    // Clean up if exists
    std.fs.cwd().deleteTree(db_path) catch {};

    std.debug.print("Creating DB at {s}...\n", .{db_path});

    // HaloDBOptions
    const options = warthogdb.WarthogDBOptions{
        .fixed_key_size = 10,
        .max_file_size = 1024 * 1024,
    };

    var db = try warthogdb.WarthogDB.open(allocator, db_path, options);
    // Ensure we close to flush everything
    defer db.close();

    try db.put("zig_key1", "zig_value1");
    try db.put("zig_key2", "zig_value2");
    try db.put("common_k", "common_v");

    std.debug.print("Wrote data: zig_key1, zig_key2, common_k\n", .{});
}

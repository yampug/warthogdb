const std = @import("std");
const root = @import("root.zig");
const WarthogDB = root.warthogdb.WarthogDB;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const db_path = "db_zig";
    std.fs.cwd().deleteTree(db_path) catch {}; // Clean start

    var db = try WarthogDB.open(allocator, db_path, .{});
    defer db.close();

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "key{d}", .{i});
        defer allocator.free(key);
        const value = try std.fmt.allocPrint(allocator, "value{d}", .{i});
        defer allocator.free(value);

        try db.put(key, value);
    }
    std.debug.print("Generated 10 records in {s}\n", .{db_path});
}

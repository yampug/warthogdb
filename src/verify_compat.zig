const std = @import("std");
const warthogdb_lib = @import("warthogdb");
const warthogdb = warthogdb_lib.warthogdb;
const Allocator = std.mem.Allocator;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const db_path = "/Users/bob/exrepos/HaloDB/cross_verify_db";

    std.debug.print("Opening DB at {s}...\n", .{db_path});

    // HaloDBOptions
    const options = warthogdb.WarthogDBOptions{
        .fixed_key_size = 10,
    };

    var db = try warthogdb.WarthogDB.open(allocator, db_path, options);
    defer db.close();

    std.debug.print("DB Opened. Verifying keys...\n", .{});

    // Verify key1
    if (try db.get("key1", allocator)) |val| {
        defer allocator.free(val);
        std.debug.print("key1 found: {s}\n", .{val});
        if (!std.mem.eql(u8, val, "value1")) {
            std.debug.print("ERROR: key1 value mismatch. Expected 'value1', got '{s}'\n", .{val});
            return error.VerificationFailed;
        }
    } else {
        std.debug.print("ERROR: key1 not found\n", .{});
        return error.VerificationFailed;
    }

    // Verify key2
    if (try db.get("key2", allocator)) |val| {
        defer allocator.free(val);
        std.debug.print("key2 found: {s}\n", .{val});
        if (!std.mem.eql(u8, val, "value2")) {
            std.debug.print("ERROR: key2 value mismatch. Expected 'value2', got '{s}'\n", .{val});
            return error.VerificationFailed;
        }
    } else {
        std.debug.print("ERROR: key2 not found\n", .{});
        return error.VerificationFailed;
    }

    // Verify longkey99
    if (try db.get("longkey99", allocator)) |val| {
        defer allocator.free(val);
        std.debug.print("longkey99 found: {s}\n", .{val});
        if (!std.mem.eql(u8, val, "longvalue99")) {
            std.debug.print("ERROR: longkey99 value mismatch. Expected 'longvalue99', got '{s}'\n", .{val});
            return error.VerificationFailed;
        }
    } else {
        std.debug.print("ERROR: longkey99 not found\n", .{});
        return error.VerificationFailed;
    }

    std.debug.print("SUCCESS: All keys verified.\n", .{});
}

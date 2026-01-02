const std = @import("std");
const warthog = @import("warthogdb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const db_path = "benchmark_native_db";
    const num_records: u32 = 100_000;

    // Clean start
    std.fs.cwd().deleteTree(db_path) catch {};

    const options = warthog.warthogdb.WarthogDBOptions{
        .fixed_key_size = 0,
        .number_of_records = num_records,
        .max_file_size = 100 * 1024 * 1024, // 100 MB
        .compaction_threshold_per_file = 0.5,
    };

    std.debug.print("Running Native Zig Benchmark...\n", .{});
    const start_open = std.time.nanoTimestamp();
    var db = try warthog.warthogdb.WarthogDB.open(allocator, db_path, options);
    defer {
        db.close();
    }
    const end_open = std.time.nanoTimestamp();
    const open_ms = @as(f64, @floatFromInt(end_open - start_open)) / 1_000_000.0;
    std.debug.print("DB Open took: {d:.2} ms\n", .{open_ms});

    // Write Benchmark
    std.debug.print("Starting Write Benchmark: {d} records\n", .{num_records});
    const start_write = std.time.nanoTimestamp();

    var buf: [20]u8 = undefined;

    var i: u32 = 0;
    while (i < num_records) : (i += 1) {
        const key = std.fmt.bufPrint(&buf, "k{d:0>8}", .{i}) catch unreachable;
        const val = std.fmt.bufPrint(buf[10..], "v{d:0>8}", .{i}) catch unreachable;
        try db.put(key, val);
    }

    const end_write = std.time.nanoTimestamp();
    const write_seconds = @as(f64, @floatFromInt(end_write - start_write)) / 1_000_000_000.0;
    const write_ops_sec = @as(f64, @floatFromInt(num_records)) / write_seconds;

    std.debug.print("Write Benchmark Finished: {d:.2} seconds\n", .{write_seconds});
    std.debug.print("Write Throughput: {d:.2} ops/sec\n", .{write_ops_sec});

    // Close and Reopen to trigger mmap on all files
    db.close();

    std.debug.print("Reopening DB for Read Benchmark...\n", .{});
    db = try warthog.warthogdb.WarthogDB.open(allocator, db_path, options);
    errdefer {
        db.close();
    }

    // Read Benchmark
    std.debug.print("Starting Read Benchmark: {d} records\n", .{num_records});
    const start_read = std.time.nanoTimestamp();

    i = 0;
    while (i < num_records) : (i += 1) {
        var k_buf: [10]u8 = undefined;
        const key = std.fmt.bufPrint(&k_buf, "k{d:0>8}", .{i}) catch unreachable;

        if (try db.getRef(key, allocator)) |view| {
            view.deinit();
        }
    }

    const end_read = std.time.nanoTimestamp();
    const read_seconds = @as(f64, @floatFromInt(end_read - start_read)) / 1_000_000_000.0;
    const read_ops_sec = @as(f64, @floatFromInt(num_records)) / read_seconds;

    std.debug.print("Read Benchmark Finished: {d:.2} seconds\n", .{read_seconds});
    std.debug.print("Read Throughput: {d:.2} ops/sec\n", .{read_ops_sec});
}

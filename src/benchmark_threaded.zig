const std = @import("std");
const warthog = @import("warthogdb.zig");
const log_file = @import("log.zig");
const record = @import("record.zig");

const NUM_THREADS = 8;
const RECORDS_PER_THREAD = 500_000;
const TOTAL_RECORDS = NUM_THREADS * RECORDS_PER_THREAD;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const db_path = "benchmark_db_threaded";
    std.fs.cwd().deleteTree(db_path) catch {};
    try std.fs.cwd().makeDir(db_path);
    // defer std.fs.cwd().deleteTree(db_path) catch {};

    const options = warthog.WarthogDBOptions{
        .fixed_key_size = 0,
        .number_of_records = TOTAL_RECORDS,
        .max_file_size = 100 * 1024 * 1024,
        .compaction_threshold_per_file = 0.5,
    };

    var db = try warthog.WarthogDB.open(allocator, db_path, options);
    // No defer close/destroy here because we might strictly control lifecycle

    // Populate Data (Single Threaded for now to ensure consistency)
    std.debug.print("Populating {d} records...\n", .{TOTAL_RECORDS});
    const start_write = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < TOTAL_RECORDS) : (i += 1) {
        var k_buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&k_buf, "k{d:0>8}", .{i}) catch unreachable;
        const val = "value_data_1234567890";
        try db.put(key, val);
    }

    const end_write = std.time.nanoTimestamp();
    const write_seconds = @as(f64, @floatFromInt(end_write - start_write)) / 1_000_000_000.0;
    std.debug.print("Write Finished: {d:.2} ops/sec\n", .{@as(f64, @floatFromInt(TOTAL_RECORDS)) / write_seconds});

    // Close and Reopen to trigger mmap
    db.close();

    std.debug.print("Reopening DB for Threaded Read Benchmark...\n", .{});
    db = try warthog.WarthogDB.open(allocator, db_path, options);
    defer db.close();

    // Threaded Read Benchmark
    const threads = try allocator.alloc(std.Thread, NUM_THREADS);
    defer allocator.free(threads);

    const context = ThreadContext{
        .db = &db,
        .records_per_thread = RECORDS_PER_THREAD,
    };

    std.debug.print("Starting {d} threads reading {d} records each...\n", .{ NUM_THREADS, RECORDS_PER_THREAD });
    const start_read = std.time.nanoTimestamp();

    for (0..NUM_THREADS) |t_idx| {
        threads[t_idx] = try std.Thread.spawn(.{}, worker, .{ context, t_idx });
    }

    for (threads) |t| {
        t.join();
    }

    const end_read = std.time.nanoTimestamp();
    const read_seconds = @as(f64, @floatFromInt(end_read - start_read)) / 1_000_000_000.0;
    const total_ops = @as(f64, @floatFromInt(TOTAL_RECORDS));

    std.debug.print("Threaded Read Finished: {d:.2} seconds\n", .{read_seconds});
    std.debug.print("Aggregate Throughput: {d:.2} ops/sec\n", .{total_ops / read_seconds});
}

const ThreadContext = struct {
    db: *warthog.WarthogDB,
    records_per_thread: usize,
};

fn worker(ctx: ThreadContext, thread_id: usize) void {
    // Thread-local allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // Each thread reads a disjoint range (or random? disjoint is easier to verify)
    const start_idx = thread_id * ctx.records_per_thread;
    const end_idx = start_idx + ctx.records_per_thread;

    // Use Thread-Local Reader
    var rwd = ctx.db.reader(allocator);
    defer rwd.deinit();

    var i: usize = start_idx;
    while (i < end_idx) : (i += 1) {
        var k_buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&k_buf, "k{d:0>8}", .{i}) catch "err";

        const maybe_view = rwd.getRef(key, allocator) catch |err| {
            std.debug.print("Error reading key {s}: {}\n", .{ key, err });
            continue;
        };

        if (maybe_view) |view| {
            view.deinit();
        }
    }
}

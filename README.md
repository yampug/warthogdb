<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/yampug/warthogdb/blob/main/github/assets/banner_dark.png?raw=true">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/yampug/warthogdb/blob/main/github/assets/banner_light.png?raw=true">
    <img alt="WarthogDB Banner" src="https://github.com/yampug/warthogdb/blob/main/github/assets/banner_light.png?raw=true">
  </picture>
</p>

[![License](https://img.shields.io/badge/license-MIT-green?labelColor=gray)](LICENSE.md)

WarthogDB is a fast and simple embedded key-value store written in Zig. It is suitable for IO bound workloads, and is capable of handling high throughput reads and writes at submillisecond latencies.

Basic design principles employed in WarthogDB are not new. At its core it started as a rewrite of Yahoo's [HaloDB](https://github.com/yahoo/HaloDB), a fast and simple embedded key-value store written in Java, in Zig. Zig was chosen for its speed and protability across different platforms and languages with its native support for building C APIs.

## Basic Operations
```zig
    const std = @import("std");
    const WarthogDB = @import("warthogdb.zig").WarthogDB;

    fn main() !void {
        const db = try WarthogDB.init(.{
            .directory = "/tmp/warthogdb",
            .max_file_size = 1024 * 1024 * 1024,
            .max_tombstone_file_size = 64 * 1024 * 1024,
            .build_index_threads = 8,
            .flush_data_size_bytes = 10 * 1024 * 1024,
            .compaction_threshold_per_file = 0.7,
            .compaction_job_rate = 50 * 1024 * 1024,
            .number_of_records = 100_000_000,
            .clean_up_tombstones_during_open = true,
            .clean_up_in_memory_index_on_close = false,
            .use_memory_pool = true,
            .memory_pool_chunk_size = 2 * 1024 * 1024,
            .fixed_key_size = 8,
        });

        defer db.deinit();

        try db.put("key", "value");
        const value = try db.get("key");
        std.log.info("value: {s}", .{value});
    }
```

## License
WarthogDB is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more information.


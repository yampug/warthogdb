const std = @import("std");
const log_file = @import("log.zig");
const Allocator = std.mem.Allocator;

pub const Snapshot = struct {
    referenced_files: std.ArrayListUnmanaged(*log_file.LogFile) = .{},
    allocator: Allocator,

    pub fn init(allocator: Allocator) Snapshot {
        return .{
            .referenced_files = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Snapshot) void {
        for (self.referenced_files.items) |file| {
            file.decref();
        }
        self.referenced_files.deinit(self.allocator);
    }

    pub fn addFile(self: *Snapshot, file: *log_file.LogFile) !void {
        file.incref();
        try self.referenced_files.append(self.allocator, file);
    }

    pub fn getFile(self: *Snapshot, file_id: u32) ?*log_file.LogFile {
        for (self.referenced_files.items) |file| {
            if (file.file_id == file_id) return file;
        }
        return null;
    }
};

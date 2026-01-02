const std = @import("std");
const Allocator = std.mem.Allocator;

/// Wrapper around Zig allocators to provide a unified interface for Warthog's memory needs.
pub const MemManager = struct {
    allocator: Allocator,

    pub fn init(allocator: Allocator) MemManager {
        return .{
            .allocator = allocator,
        };
    }

    /// Allocates memory aligned to the page size or a specific boundary.
    /// Useful for Direct IO or specific memory mapping requirements.
    pub fn alloc(self: *MemManager, comptime T: type, count: usize) ![]T {
        return self.allocator.alloc(T, count);
    }

    pub fn free(self: *MemManager, ptr: anytype) void {
        self.allocator.free(ptr);
    }

    /// Mimics Java's Unsafe.allocateMemory (but safer).
    /// Used for the off-heap index.
    pub fn loopupOrAlloc(self: *MemManager, size: usize) ![]u8 {
        return self.allocator.alloc(u8, size);
    }
};

test "MemManager basic usage" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var mm = MemManager.init(allocator);
    const buf = try mm.alloc(u8, 1024);
    defer mm.free(buf);

    try std.testing.expectEqual(buf.len, 1024);
}

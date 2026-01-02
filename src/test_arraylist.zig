const std = @import("std");

test "Inspect ArrayList Details" {
    const T = std.ArrayList(u32);
    const has_allocator = @hasField(T, "allocator");
    const has_init = @hasDecl(T, "init");
    @compileLog("Has allocator:", has_allocator);
    @compileLog("Has init:", has_init);
}

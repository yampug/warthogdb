const std = @import("std");
test "sleep" {
    std.Thread.sleep(100);
}

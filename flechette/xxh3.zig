const std = @import("std");

pub const R = u64;

ctx: std.hash.XxHash3 = .init(0),

pub fn roll(self: *@This(), bytes: []const u8) void {
    self.ctx.update(bytes);
}

pub fn final(self: *@This()) R {
    return self.ctx.final();
}

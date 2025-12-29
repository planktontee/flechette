const std = @import("std");
const c = @import("c.zig").c;

pub const R = [c.SHA256_DIGEST_LENGTH]u8;

ctx: c.SHA256_CTX = undefined,
result: R = undefined,

pub fn init() @This() {
    var self: @This() = .{};
    if (c.SHA256_Init(&self.ctx) == 0) {
        @branchHint(.cold);
        @panic("Failed to init SHA256");
    }
    return self;
}

pub fn final(self: *@This()) R {
    @setRuntimeSafety(false);
    if (c.SHA256_Final(&self.result, &self.ctx) == 0) {
        @branchHint(.cold);
        @panic("Failed to finalize SHA256");
    }
    return self.result;
}

pub fn roll(self: *@This(), data: []const u8) void {
    @setRuntimeSafety(false);
    if (c.SHA256_Update(&self.ctx, @ptrCast(@alignCast(data.ptr)), data.len) == 0) {
        @branchHint(.cold);
        @panic("Failed to update SHA256");
    }
}

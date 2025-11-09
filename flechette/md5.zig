const std = @import("std");
const Md5 = std.crypto.hash.Md5;
const c = @import("c.zig").c;

pub const R = [16]u8;

ctx: c.MD5_CTX = undefined,
// md5: Md5 = undefined,
result: R = undefined,

pub fn init() @This() {
    var self: @This() = .{};
    if (c.MD5_Init(&self.ctx) == 0) {
        @panic("Failed to init MD5");
    }
    return self;
    // return .{
    // .md5 = .init(.{}),
    // };
}

pub fn final(self: *@This()) R {
    @setRuntimeSafety(false);
    if (c.MD5_Final(&self.result, &self.ctx) == 0) {
        @panic("Failed to finalize MD5");
    }
    // self.md5.final(&self.result);
    return self.result;
}

pub fn roll(self: *@This(), data: []const u8) void {
    @setRuntimeSafety(false);
    if (c.MD5_Update(&self.ctx, @ptrCast(@alignCast(data.ptr)), data.len) == 0) {
        @panic("Failed to update MD5");
    }
    // self.md5.update(data);
}

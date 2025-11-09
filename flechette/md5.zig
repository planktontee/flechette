const std = @import("std");
// TODO: use SIMD, zig's implementation is unfortunately very slow
const Md5 = std.crypto.hash.Md5;

pub const R = [16]u8;

result: R = @splat(0),
md5: Md5 = .init(.{}),

pub fn roll(self: *@This(), data: []const u8) void {
    self.md5.update(data);
}

pub fn final(self: *@This()) R {
    self.md5.final(&self.result);
    return self.result;
}

const std = @import("std");
const c = @import("c.zig").c;

// NOTE:
// THis might not work other OpenSSL hashes, I browsed through the repository
// and it seems only md and sha families use this style? - needs to be seen

pub fn OpensslCtxT(T: type) type {
    return struct {
        fnInit: fn (c: [*c]T) callconv(.c) c_int,
        fnUpdate: fn (c: [*c]T, data: ?*const anyopaque, len: usize) callconv(.c) c_int,
        fnFinal: fn (md: [*c]u8, c: [*c]T) callconv(.c) c_int,
    };
}

pub fn Hash(T: type, CtxT: OpensslCtxT(T), digestLen: comptime_int) type {
    return struct {
        pub const R = [digestLen]u8;

        ctx: T = undefined,
        result: R = undefined,

        pub fn init() @This() {
            var self: @This() = .{};
            if (CtxT.fnInit(&self.ctx) == 0) {
                @branchHint(.cold);
                @panic("Failed to init SHA256");
            }
            return self;
        }

        pub fn final(self: *@This()) R {
            @setRuntimeSafety(false);
            if (CtxT.fnFinal(&self.result, &self.ctx) == 0) {
                @branchHint(.cold);
                @panic("Failed to finalize SHA256");
            }
            return self.result;
        }

        pub fn roll(self: *@This(), data: []const u8) void {
            @setRuntimeSafety(false);
            if (CtxT.fnUpdate(&self.ctx, @ptrCast(@alignCast(data.ptr)), data.len) == 0) {
                @branchHint(.cold);
                @panic("Failed to update SHA256");
            }
        }
    };
}

pub const MD5 = Hash(
    c.MD5_CTX,
    .{
        .fnInit = c.MD5_Init,
        .fnUpdate = c.MD5_Update,
        .fnFinal = c.MD5_Final,
    },
    c.MD5_DIGEST_LENGTH,
);

pub const SHA256 = Hash(
    c.SHA256_CTX,
    .{
        .fnInit = c.SHA256_Init,
        .fnUpdate = c.SHA256_Update,
        .fnFinal = c.SHA256_Final,
    },
    c.SHA256_DIGEST_LENGTH,
);

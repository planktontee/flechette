const std = @import("std");
const blake2 = std.crypto.hash.blake2;
const Allocator = std.mem.Allocator;

pub const R = []u8;

const Blake2b = @This();

pub const Type = enum {
    @"64",
    @"128",
    @"160",
    @"256",
    @"384",
    @"512",
};

fn Blake2bInner(t: Type) type {
    return struct {
        ctx: switch (t) {
            .@"64" => blake2.Blake2b(64),
            .@"128" => blake2.Blake2b128,
            .@"160" => blake2.Blake2b160,
            .@"256" => blake2.Blake2b256,
            .@"384" => blake2.Blake2b384,
            .@"512" => blake2.Blake2b512,
        } = .init(.{}),
        out: [
            switch (t) {
                .@"64" => 64,
                .@"128" => 128,
                .@"160" => 160,
                .@"256" => 256,
                .@"384" => 384,
                .@"512" => 512,
            } / 8
        ]u8 = undefined,

        pub const vtable: @FieldType(Blake2b, "vtable") = &.{
            .roll = &@This().roll,
            .final = &@This().final,
            .deinit = &@This().deinit,
        };

        pub fn roll(selfOpaque: *anyopaque, bytes: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(selfOpaque));
            self.ctx.update(bytes);
        }

        pub fn final(selfOpaque: *anyopaque) R {
            const self: *@This() = @ptrCast(@alignCast(selfOpaque));
            self.ctx.final(&self.out);
            return self.out[0..];
        }

        pub fn deinit(selfOpaque: *anyopaque, allocator: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(selfOpaque));
            allocator.destroy(self);
        }

        pub fn asBlake2b(self: *@This()) Blake2b {
            return .{
                .ctx = @ptrCast(@alignCast(self)),
                .vtable = vtable,
                .allocator = undefined,
            };
        }
    };
}

ctx: *anyopaque = undefined,
vtable: *const struct {
    roll: *const fn (*anyopaque, []const u8) void,
    final: *const fn (*anyopaque) R,
    deinit: *const fn (*anyopaque, allocator: Allocator) void,
} = undefined,
allocator: Allocator,

pub fn init(t: Type, allocator: Allocator) !@This() {
    inline for (std.meta.fields(Type)) |field| {
        if (std.mem.eql(u8, field.name, @tagName(t))) {
            const inst = try allocator.create(Blake2bInner(@enumFromInt(field.value)));
            inst.* = .{};
            var blake2b = inst.asBlake2b();
            blake2b.allocator = allocator;
            return blake2b;
        }
    }
    return error{UnknownBlake2bFlavour}.UnknownBlake2bFlavour;
}

pub fn roll(self: *@This(), bytes: []const u8) void {
    self.vtable.roll(self.ctx, bytes);
}

pub fn final(self: *@This()) R {
    return self.vtable.final(self.ctx);
}

pub fn deinit(self: *@This()) void {
    self.vtable.deinit(self.ctx, self.allocator);
    self.ctx = undefined;
    self.vtable = undefined;
}

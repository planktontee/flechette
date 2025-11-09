pub const std = @import("std");

pub const AdlerType = enum {
    adler32,
    adler64,

    pub fn hashT(comptime adlerType: *const @This()) type {
        return switch (adlerType.*) {
            .adler32 => u32,
            .adler64 => u64,
        };
    }
};

pub fn AdlerHash(comptime adlerType: AdlerType) type {
    const T = adlerType.hashT();
    // 2 ^ bits closer smaller closest prime
    // nmax is the largest n such that 255n(n+1)/2 + (n+1)(base-1) does not overflow
    const Base: T, const Nmax: comptime_int, const Shift: comptime_int, const Mask: comptime_int = comptime switch (adlerType) {
        .adler32 => .{
            0xFFF1,
            5552,
            16,
            0xFFFF,
        },
        .adler64 => .{
            0xFFFFFFFB,
            363898415,
            32,
            0xFFFFFFFF,
        },
    };
    const VecLen = std.simd.suggestVectorLength(u16) orelse 1;
    const VecT = @Vector(VecLen, u16);
    const RBits = Mask - Base + 1;

    return struct {
        pub const R = T;
        result: T = 1,
        // Stolen from a zig branch optimizing adler32 and then changed to work with adler64
        // https://github.com/ziglang/zig/blob/b27e2ab0afde4aee9d8bc704a05946117ea36a38/lib/std/hash/Adler32.zig
        fn innerDigest(data: []const u8, a: T, b: T) T {
            const rounds = Nmax / VecLen;

            var s1: T = a;
            var s2: T = b;

            var i: usize = 0;
            while (i + Nmax <= data.len) {
                for (0..rounds) |_| {
                    const vec: VecT = data[i..][0..VecLen].*;

                    s2 += VecLen * s1;
                    s1 += @reduce(.Add, vec);
                    // This is faster than precomputing the table even with prefetch
                    s2 += @reduce(.Add, vec * std.simd.reverseOrder(
                        std.simd.iota(u32, VecLen) + @as(VecT, @splat(1)),
                    ));

                    i += VecLen;
                }

                // This seems to be as slow as s1 %= s1;
                s1 = RBits * (s1 >> Shift) + (s1 & Mask);
                s2 = RBits * (s2 >> Shift) + (s2 & Mask);
            }

            while (i + VecLen <= data.len) : (i += VecLen) {
                const vec: VecT = data[i..][0..VecLen].*;

                s2 += VecLen * s1;
                s1 += @reduce(.Add, vec);
                s2 += @reduce(.Add, vec * std.simd.reverseOrder(
                    std.simd.iota(u32, VecLen) + @as(VecT, @splat(1)),
                ));
            }

            for (data[i..]) |byte| {
                s1 += byte;
                s2 += s1;
            }

            s1 = RBits * (s1 >> Shift) + (s1 & Mask);
            s2 = RBits * (s2 >> Shift) + (s2 & Mask);

            return ((s2 % Base) << Shift) | (s1 % Base);
        }

        pub fn roll(self: *@This(), data: []const u8) void {
            // TODO: add option to combine
            self.result = innerDigest(data, self.result & Mask, (self.result >> Shift) & Mask);
        }

        pub fn final(self: *const @This()) T {
            return self.result;
        }
    };
}

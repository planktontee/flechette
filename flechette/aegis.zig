// Thanks Protty :D

const Aegis = Aegis128X(
    @divExact(std.simd.suggestVectorLength(u8).?, 16),
    128,
);

const std = @import("std");
const builtin = @import("builtin");

fn Aegis128X(degree: comptime_int, tag_bits: comptime_int) type {
    return struct {
        state: [8]V,

        const Self = @This();
        const V = @Vector(16 * degree, u8);

        pub const Hasher = struct {
            state: Self = init(@splat(0), @splat(0)),
            buf: [n]u8 = @splat(0),
            len: usize = 0,
            total: usize = 0,

            const n = @sizeOf(V) * 2;

            fn copy(noalias dst: [*]u8, noalias src: [*]const u8, len: usize) void {
                if (len < 4) {
                    @branchHint(.unlikely);
                    for ([_]usize{ 0, len / 2, len - 1 }) |i| dst[i] = src[i];
                } else if (len <= 16) {
                    @branchHint(.unlikely);
                    const mid = (len / 8) * 4;
                    for ([_]usize{ 0, mid, len - 4, len - 4 - mid }) |i|
                        dst[i..][0..4].* = src[i..][0..4].*;
                } else if (n <= 64 or len <= 64) {
                    const mid = (len / 32) * 16;
                    for ([_]usize{ 0, mid, len - 16, len - 16 - mid }) |i|
                        dst[i..][0..16].* = src[i..][0..16].*;
                } else if (comptime n > 64) {
                    @branchHint(.unlikely);
                    comptime std.debug.assert(n == 128);
                    for ([_]usize{ 0, len - 64 }) |i|
                        dst[i..][0..64].* = src[i..][0..64].*;
                }
            }

            pub fn roll(self: *Hasher, bytes: []const u8) void {
                if (bytes.len == 0) return;
                self.total += bytes.len;

                if (self.len + bytes.len <= self.buf.len) {
                    copy(self.buf[0..].ptr + self.len, bytes.ptr, bytes.len);
                    self.len += bytes.len;
                    return;
                }

                var in = bytes;
                if (self.len > 0) {
                    const fill = self.buf.len - self.len;
                    if (fill != 0) {
                        copy(self.buf[0..].ptr + self.len, in.ptr, fill);
                        in = in[fill..];
                    }
                    const m: *align(1) const [2]V = @ptrCast(&self.buf);
                    self.state.update(m[0], m[1]);
                }
                while (in.len > n * 4) : (in = in[n * 4 ..]) {
                    inline for (0..4) |i| {
                        const m: *align(1) const [2]V = @ptrCast(in[i * n ..][0..n]);
                        self.state.update(m[0], m[1]);
                    }
                }
                while (in.len > self.buf.len) : (in = in[self.buf.len..]) {
                    const m: *align(1) const [2]V = @ptrCast(in[0..self.buf.len]);
                    self.state.update(m[0], m[1]);
                }

                self.len = in.len;
                self.buf = @splat(0);
                copy(self.buf[0..].ptr, in.ptr, self.len);
            }

            pub fn final(self: *Hasher) [@divExact(tag_bits, 8)]u8 {
                if (self.len > 0) {
                    self.len = 0;
                    const m: *align(1) const [2]V = @ptrCast(&self.buf);
                    self.state.update(m[0], m[1]);
                }

                return self.state.final(self.total, 0);
            }
        };

        pub fn hash(bytes: []const u8) [@divExact(tag_bits, 8)]u8 {
            var in = bytes;
            var self = comptime init(@splat(0), @splat(0));

            // absorb(): https://cfrg.github.io/draft-irtf-cfrg-aegis-aead/draft-irtf-cfrg-aegis-aead.html#section-5.4.3
            const ad_len = in.len;
            const n = @sizeOf(V) * 2;
            while (in.len >= n) : (in = in[n..]) {
                const m: [2]V = @bitCast(in[0..n].*);
                self.update(m[0], m[1]);
            }

            if (in.len > 0) {
                @branchHint(.likely);

                // branch optimized reading of last chunk.
                var last: [n]u8 = @splat(0);
                if (in.len < 4) {
                    @branchHint(.unlikely);
                    for ([_]usize{ 0, in.len / 2, in.len - 1 }) |i| last[i] = in[i];
                } else if (in.len <= 16) {
                    @branchHint(.unlikely);
                    const mid = (in.len / 8) * 4;
                    for ([_]usize{ 0, mid, in.len - 4, in.len - 4 - mid }) |i|
                        last[i..][0..4].* = in[i..][0..4].*;
                } else if (n <= 64 or in.len <= 64) {
                    const mid = (in.len / 32) * 16;
                    for ([_]usize{ 0, mid, in.len - 16, in.len - 16 - mid }) |i|
                        last[i..][0..16].* = in[i..][0..16].*;
                } else if (comptime n > 64) {
                    @branchHint(.unlikely);
                    comptime std.debug.assert(n == 128);
                    for ([_]usize{ 0, in.len - 64 }) |i|
                        last[i..][0..64].* = in[i..][0..64].*;
                }

                const m: [2]V = @bitCast(last[0..n].*);
                self.update(m[0], m[1]);
            }

            return self.final(ad_len, 0);
        }

        // https://cfrg.github.io/draft-irtf-cfrg-aegis-aead/draft-irtf-cfrg-aegis-aead.html#section-5.4.2
        fn init(key: [16]u8, nonce: [16]u8) Self {
            const k: V = @bitCast(@as([degree][16]u8, @splat(key)));
            const n: V = @bitCast(@as([degree][16]u8, @splat(nonce)));
            const c0: V = @bitCast(@as([degree][16]u8, @splat([_]u8{ 0x00, 0x01, 0x01, 0x02, 0x03, 0x05, 0x08, 0x0d, 0x15, 0x22, 0x37, 0x59, 0x90, 0xe9, 0x79, 0x62 })));
            const c1: V = @bitCast(@as([degree][16]u8, @splat([_]u8{ 0xdb, 0x3d, 0x18, 0x55, 0x6d, 0xc2, 0x2f, 0xf1, 0x20, 0x11, 0x31, 0x42, 0x73, 0xb5, 0x28, 0xdd })));

            var self: Self = .{ .state = .{
                k ^ n,
                c1,
                c0,
                c1,
                k ^ n,
                k ^ c0,
                k ^ c1,
                k ^ c0,
            } };

            var _ctx: [degree][16]u8 = undefined;
            for (0..degree) |d| _ctx[d] = [_]u8{ @intCast(d), degree - 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
            const ctx: V = @bitCast(_ctx);

            for (0..10) |_| {
                self.state[3] ^= ctx;
                self.state[7] ^= ctx;
                self.update(n, k);
            }

            return self;
        }

        // https://cfrg.github.io/draft-irtf-cfrg-aegis-aead/draft-irtf-cfrg-aegis-aead.html#section-5.4.1
        fn update(self: *Self, m0: V, m1: V) void {
            const s = self.state;
            self.state = .{
                aesenc(s[7], s[0] ^ m0),
                aesenc(s[0], s[1]),
                aesenc(s[1], s[2]),
                aesenc(s[2], s[3]),
                aesenc(s[3], s[4] ^ m1),
                aesenc(s[4], s[5]),
                aesenc(s[5], s[6]),
                aesenc(s[6], s[7]),
            };
        }

        // https://cfrg.github.io/draft-irtf-cfrg-aegis-aead/draft-irtf-cfrg-aegis-aead.html#section-5.4.7
        fn final(self: *Self, ad_len: u64, enc_len: u64) [@divExact(tag_bits, 8)]u8 {
            var t0: [degree]@Vector(16, u8) =
                @splat(@as(@Vector(16, u8), @bitCast([_]u64{ ad_len, enc_len })));

            for (0..7) |_|
                self.update(@bitCast(t0), @bitCast(t0));

            const s = &self.state;
            switch (tag_bits) {
                128 => {
                    t0 = @bitCast(s[0] ^ s[1] ^ s[2] ^ s[3] ^ s[4] ^ s[5] ^ s[6] ^ s[7]);
                    inline for (1..degree) |d| t0[0] ^= t0[d];
                    return @bitCast(t0[0]);
                },
                256 => {
                    t0 = @bitCast(s[0] ^ s[1] ^ s[2] ^ s[3]);
                    const t1: @TypeOf(t0) = @bitCast(s[4] ^ s[5] ^ s[6] ^ s[7]);
                    inline for (1..degree) |d| {
                        t0[0] ^= t0[d];
                        t1[0] ^= t1[d];
                    }
                    return @bitCast([_]@Vector(16, u8){ t0[0], t1[1] });
                },
                else => @compileError("invalid tag_bits (must be 128 or 256)"),
            }
        }

        inline fn aesenc(block: V, round_key: V) V {
            @setEvalBranchQuota(100000);
            if (!@inComptime()) {
                // x86 impl
                if (comptime builtin.cpu.arch.isX86() and
                    std.Target.x86.featureSetHas(builtin.cpu.features, .aes))
                {
                    const n = comptime if (degree >= 4 and std.Target.x86.featureSetHas(builtin.cpu.features, .avx512f)) 4 else if (degree >= 2 and std.Target.x86.featureSetHas(builtin.cpu.features, .avx2)) 2 else 1;

                    var v: [@divExact(degree, n)]@Vector(16 * n, u8) = undefined;
                    inline for (0..v.len) |i| {
                        v[i] = asm (
                            \\ vaesenc %[rk], %[in], %[out]
                            : [out] "=x" (-> @TypeOf(v[i])),
                            : [in] "x" (@as(@TypeOf(v), @bitCast(block))[i]),
                              [rk] "x" (@as(@TypeOf(v), @bitCast(round_key))[i]),
                        );
                    }
                    return @bitCast(v);
                }

                // ARM impl
                if (comptime builtin.cpu.arch.isAARCH64() and
                    std.Target.aarch64.featureSetHas(builtin.cpu.features, .aes))
                {
                    var v: [degree]@Vector(16, u8) = @bitCast(block);
                    inline for (0..degree) |i| {
                        v[i] = asm (
                            \\ aese  %[in].16b, %[zero].16b
                            \\ aesmc %[out].16b, %[in].16b
                            : [out] "=&x" (-> @Vector(16, u8)),
                            : [in] "x" (v[i]),
                              [zero] "x" (@as(@Vector(16, u8), @splat(0))),
                        ) ^ @as([degree]@Vector(16, u8), @bitCast(round_key))[i];
                    }
                    return @bitCast(v);
                }
            }

            // software
            var v: [degree][4]u32 = @bitCast(block);
            for (0..degree) |i| {
                var s: [4]u32 = @bitCast(v[i]);
                var t: [4]u32 = @bitCast(@as([degree][4]u32, @bitCast(round_key))[i]);
                for (0..4) |j| {
                    for (0..4) |k| t[k] ^= table[j][s[(k + j) % 4] & 0xff];
                    for (0..4) |k| s[k] >>= 8;
                }
                v[i] = @bitCast(t);
            }
            return @bitCast(v);
        }

        const table = blk: {
            @setEvalBranchQuota(100000);

            // generateSbox(invert = false)
            var sbox: [256]u8 = undefined;
            var p: u8, var q: u8 = .{ 1, 1 };
            for (0..256) |_| {
                p = mul(p, 3);
                q = mul(q, 0xf6);
                var v = q ^ 0x63;
                for (0..4) |i| v ^= std.math.rotl(u8, q, i + 1);
                sbox[p] = v;
            }
            sbox[0] = 0x63;

            // generateTable(invert = false)
            var tbl: [4][256]u32 = undefined;
            for (sbox, 0..) |v, i| {
                tbl[0][i] = @bitCast([4]u8{ mul(v, 2), mul(v, 1), mul(v, 1), mul(v, 3) });
                for (1..4) |j| tbl[j][i] = std.math.rotl(u32, tbl[0][i], j * 8);
            }
            break :blk tbl;
        };

        // Multiply a and b as GF(2) polynomials modulo poly.
        fn mul(a: u8, b: u8) u8 {
            @setEvalBranchQuota(30000);

            // Rijndael's irreducible polynomial.
            const poly: u9 = 1 << 8 | 1 << 4 | 1 << 3 | 1 << 1 | 1 << 0; // x⁸ + x⁴ + x³ + x + 1

            var i: u8, var j: u9, var s: u9 = .{ a, b, 0 };
            while (i > 0) : (i >>= 1) {
                if (i & 1 != 0) s ^= j;
                j *= 2;
                if (j & 0x100 != 0) j ^= poly;
            }
            return @truncate(s);
        }
    };
}

pub const R = [@divExact(128, 8)]u8;

ctx: Aegis.Hasher = .{},

pub fn roll(self: *@This(), bytes: []const u8) void {
    self.ctx.roll(bytes);
}

pub fn final(self: *@This()) R {
    return self.ctx.final();
}

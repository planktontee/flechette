// Thanks Protty :D

const std = @import("std");

pub const RapidHash = struct {
    const R = u64;
    const Self = @This();
    const sk = [_]u64{
        0x2d358dccaa6c78a5,
        0x8bb84b93962eacc9,
        0x4b33a62ed433d4a3,
        0x4d5a2da51de1aa47,
        0xa0761d6478bd642f,
        0xe7037ed1a0b428db,
        0x90ed1765281c388c,
        0xaaaaaaaaaaaaaaaa,
    };

    s: [7]u64,
    len: u32 = 0,
    total: u32 = 0,
    buf: [16 * 7]u8 = undefined,

    pub fn init(seed: u64) Self {
        return .{ .s = @splat(seed) };
    }

    pub fn roll(self: *Self, bytes: []const u8) void {
        if (bytes.len == 0) return;
        self.total += @intCast(bytes.len);

        if (self.len + bytes.len <= self.buf.len) {
            @memcpy(self.buf[self.len..][0..bytes.len], bytes);
            self.len += @intCast(bytes.len);
            return;
        }

        var in = bytes;
        if (self.len > 0) {
            @memcpy(self.buf[self.len..], in[0 .. self.buf.len - self.len]);
            in = in[self.buf.len - self.len ..];
            self.round(@ptrCast(&self.buf));
        }

        while (in.len > self.buf.len * 2) : (in = in[self.buf.len * 2 ..]) {
            @call(.always_inline, round, .{ self, @as(MsgPtr, @ptrCast(in[0..self.buf.len])) });
            @call(.always_inline, round, .{ self, @as(MsgPtr, @ptrCast(in[self.buf.len..][0..self.buf.len])) });
        }

        if (in.len > self.buf.len) {
            self.round(@ptrCast(in[0..self.buf.len]));
            in = in[self.buf.len..];
        }

        self.len = @intCast(in.len);
        @memcpy(self.buf[0..in.len], in);
    }

    inline fn mix(a: u64, b: u64) u64 {
        const m: [2]u64 = @bitCast(@as(u128, a) * b);
        return m[0] ^ m[1];
    }

    const MsgPtr = *align(1) const [7][2]u64;

    inline fn round(self: *Self, msg: MsgPtr) void {
        inline for (0..7) |i| {
            self.s[i] = mix(msg[i][0] ^ sk[i], msg[i][1] ^ self.s[i]);
        }
    }

    inline fn hashLast(self: *const Self, i: comptime_int, s0: u64) u64 {
        if (i == 6 or s0 <= i * 16) return s0;
        const s1 = mix(
            @as(u64, @bitCast(self.buf[i * 16 ..][0..8].*)) ^ sk[@as(u8, 1) + @intFromBool(i <= 1 or i == 4)],
            @as(u64, @bitCast(self.buf[i * 16 + 8 ..][0..8].*)) ^ s0,
        );
        return self.hashLast(i + 1, s1);
    }

    pub fn final(self: *const Self) @This().R {
        var s = self.s[0];
        var m: [2]u64 = @splat(0);

        if (self.total > 0) {
            if (self.total > self.buf.len) {
                s ^= self.s[1];
                const s2 = self.s[2] ^ self.s[3];
                const s4 = self.s[4] ^ self.s[5];
                s ^= self.s[6];
                s ^= (s2 ^ s4);
            }

            if (self.len <= 16) {
                @branchHint(.likely);
                if (self.len >= 4) {
                    s ^= self.total;
                    if (self.len >= 8) {
                        m[0] = @bitCast(self.buf[0..8].*);
                        m[1] = @bitCast(self.buf[self.len - 8 ..][0..8].*);
                    } else {
                        m[0] = @as(u32, @bitCast(self.buf[0..4].*));
                        m[1] = @as(u32, @bitCast(self.buf[self.len - 4 ..][0..4].*));
                    }
                } else {
                    m[0] = (@as(u64, self.buf[0]) << 45) | self.buf[self.len - 1];
                    m[1] = self.buf[self.len / 2];
                }
            } else {
                s = self.hashLast(0, s);
                m[0] = @bitCast(self.buf[self.len - 16 ..][0..8].*);
                m[1] = @bitCast(self.buf[self.len - 8 ..][0..8].*);
            }
        }

        m = @bitCast(@as(u128, m[0] ^ sk[1]) * (m[1] ^ s));
        return mix(m[0] ^ sk[7], m[1] ^ sk[1] ^ self.total);
    }
};

ctx: RapidHash = .init(0),

pub const R = RapidHash.R;

pub fn roll(self: *@This(), bytes: []const u8) void {
    self.ctx.roll(bytes);
}

pub fn final(self: *@This()) R {
    return self.ctx.final();
}

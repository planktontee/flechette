const std = @import("std");

// Stolen from a zig branch optimizing adler32 and then changed to work with adler64
// https://github.com/ziglang/zig/blob/b27e2ab0afde4aee9d8bc704a05946117ea36a38/lib/std/hash/Adler32.zig
fn adler64_vector(data: []const u8, a_start: u64, b_start: u64) struct { u64, u64 } {
    const base: u64 = 0xFFFFFFFB;

    // nmax is the largest n such that 255n(n+1)/2 + (n+1)(base-1) does not overflow
    const nmax: comptime_int = 55517953;

    var s1: u64 = a_start;
    var s2: u64 = b_start;

    const vec_len = std.simd.suggestVectorLength(u16) orelse 1;
    const Vec = @Vector(vec_len, u16);

    var i: usize = 0;

    while (i + nmax <= data.len) {
        const rounds = nmax / vec_len;
        for (0..rounds) |_| {
            const vec: Vec = data[i..][0..vec_len].*;

            s2 += vec_len * s1;
            s1 += @reduce(.Add, vec);
            s2 += @reduce(.Add, vec * std.simd.reverseOrder(
                std.simd.iota(
                    u32,
                    vec_len,
                ) + @as(Vec, @splat(1)),
            ));

            i += vec_len;
        }

        s1 %= base;
        s2 %= base;
    }

    while (i + vec_len <= data.len) : (i += vec_len) {
        const vec: Vec = data[i..][0..vec_len].*;

        s2 += vec_len * s1;
        s1 += @reduce(.Add, vec);
        s2 += @reduce(.Add, vec * std.simd.reverseOrder(
            std.simd.iota(u32, vec_len) + @as(Vec, @splat(1)),
        ));
    }

    for (data[i..]) |byte| {
        s1 += byte;
        s2 += s1;
    }

    s1 %= base;
    s2 %= base;

    return .{ s2, s1 };
}

/// Print a performance report
fn print_report(
    chunk: usize,
    writer: *std.Io.Writer,
    hash: u64,
    nanos: f64,
    dsize: f64,
    size: f64,
    total: f64,
) !void {
    const kb = dsize / 1024.0;
    const mb = kb / 1024.0;
    const gb = mb / 1024.0;
    const seconds = nanos / 1_000_000_000.0;
    try writer.print(
        "[{d}]: Hash: 0x{x} : {d:.2}ms : {d:.2}B/s : {d:.2}kB/s : {d:.2}MB/s : {d:.2}GB/s : {d:.2}%\n",
        .{
            chunk,
            hash,
            nanos / 1_000_000.0,
            dsize / seconds,
            kb / seconds,
            mb / seconds,
            gb / seconds,
            (total / size * 100.0),
        },
    );
}

pub fn main() !void {
    var args = std.process.args();
    _ = args.next().?;

    var pathBuf: [1024]u8 = undefined;
    const path = try std.fs.cwd().realpath(args.next().?, &pathBuf);

    const f = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });
    const stat = try f.stat();
    const fileSize = stat.size;

    var errBuff: [512]u8 = undefined;
    // const ptr = try std.posix.mmap(
    //     null,
    //     stat.size,
    //     std.posix.PROT.READ,
    //     .{
    //         .TYPE = .PRIVATE,
    //         .POPULATE = true,
    //     },
    //     f.handle,
    //     0,
    // );

    const buffLen = 50 * (1 << 17);
    var buff: [buffLen]u8 = undefined;
    var reader = std.fs.File.reader(f, &buff);
    var stderr = std.fs.File.stderr().writer(&errBuff);
    // for (ptr) |b| try stderr.interface.print("{x:0<2}", .{b});
    // _ = try stderr.interface.write("\n");
    // try stderr.interface.flush();

    var i: usize = 0;
    var t0 = try std.time.Timer.start();
    var total: u64 = 0;
    while (true) {
        // try reader.interface.fill(buffLen - 1);
        var timer = try std.time.Timer.start();
        var buffInner: [4098]u8 = undefined;
        const n = reader.read(&buffInner) catch break;
        const a, const b = adler64_vector(buffInner[0..n], 1, 0);
        const hash = (a << 32) | b;
        const nanos = timer.read();
        i += 1;
        total += n;
        if (std.crypto.random.intRangeAtMost(u32, 0, 10e4) == 5) {
            try print_report(
                i,
                &stderr.interface,
                hash,
                @floatFromInt(nanos),
                @floatFromInt(n),
                @floatFromInt(fileSize),
                @floatFromInt(total),
            );
            try stderr.interface.flush();
        }
    }
    try stderr.interface.print("Elapsed {d:.2}s\n", .{@as(f128, @floatFromInt(t0.read())) / 10.0e7});
    try stderr.interface.flush();
}

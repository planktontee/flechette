const std = @import("std");
const zpec = @import("zpec");
const args = zpec.args;
const spec = args.spec;
const positionals = args.positionals;
const HelpData = args.help.HelpData;
const PositionalOf = positionals.PositionalOf;
const SpecResponse = spec.SpecResponse;
const Cursor = zpec.collections.Cursor;
const AsCursor = zpec.collections.AsCursor;

fn AdlerHash(comptime adlerType: AdlerType) type {
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
        hash: T = 1,
        // Stolen from a zig branch optimizing adler32 and then changed to work with adler64
        // https://github.com/ziglang/zig/blob/b27e2ab0afde4aee9d8bc704a05946117ea36a38/lib/std/hash/Adler32.zig
        pub fn digest(data: []const u8, a: T, b: T) T {
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
            self.hash = digest(data, self.hash & Mask, (self.hash >> Shift) & Mask);
        }
    };
}

fn printReport(
    comptime adlerType: AdlerType,
    chunkIndex: usize,
    writer: *std.Io.Writer,
    hash: adlerType.hashT(),
    elapsed: u64,
    fileSize: u64,
    bytesProcessed: u64,
) !void {
    const elapsedF: f128 = @floatFromInt(elapsed);
    const bytesProcessedF: f128 = @floatFromInt(bytesProcessed);
    const elapsedInSec = elapsedF / NanoUnit.s;
    try writer.print(
        "{s} digest: 0x{x}, elasped {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s, chunk {d} {d:.2}%\n",
        .{
            @tagName(adlerType),
            hash,
            elapsedInSec,
            elapsedF / NanoUnit.ms,
            bytesProcessedF / ByteUnit.mb / elapsedInSec,
            bytesProcessedF / ByteUnit.gb / elapsedInSec,
            chunkIndex,
            bytesProcessedF / @as(f128, @floatFromInt(fileSize)) * 100.0,
        },
    );
}

pub fn ioWithMmap(w: *std.Io.Writer, path: []const u8, comptime adlerType: AdlerType) !void {
    var timer = try std.time.Timer.start();

    var hasher: AdlerHash(adlerType) = .{};

    const f = try std.fs.openFileAbsolute(path, .{
        .mode = .read_only,
    });
    const stat = try f.stat();
    const fileSize = stat.size;

    const ptr = try std.posix.mmap(
        null,
        stat.size,
        std.posix.PROT.READ,
        .{
            .TYPE = .PRIVATE,
            .NONBLOCK = true,
        },
        f.handle,
        0,
    );

    hasher.roll(ptr);

    try printReport(
        adlerType,
        0,
        w,
        hasher.hash,
        timer.read(),
        fileSize,
        fileSize,
    );
    try w.flush();
}

const NanoUnit = struct {
    pub const ms = 10e5;
    pub const s = 10e8;
};

const ByteUnit = struct {
    pub const mb = 1 << 20;
    pub const gb = 1 << 30;
};

pub fn ioWithBuffer(w: *std.Io.Writer, path: []const u8, comptime adlerType: AdlerType) !void {
    var timer = try std.time.Timer.start();

    var hasher: AdlerHash(adlerType) = .{};

    const f = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });

    // This does nothing for bigger files
    const buffLen = ByteUnit.mb * 2;
    const buff = try std.heap.page_allocator.alloc(u8, buffLen);
    defer std.heap.page_allocator.free(buff);

    const fstat = try f.stat();
    const fileSize = fstat.size;

    var reader = std.fs.File.reader(f, &.{});

    var chunkIndex: usize = 0;
    var bytesProcessed: u64 = 0;

    while (true) {
        const chunkLen = reader.read(buff) catch |e| switch (e) {
            error.EndOfStream => break,
            error.ReadFailed => return e,
        };
        hasher.roll(buff);

        chunkIndex += 1;
        bytesProcessed += chunkLen;
    }

    try printReport(
        adlerType,
        chunkIndex,
        w,
        hasher.hash,
        timer.read(),
        fileSize,
        bytesProcessed,
    );
    try w.flush();
}

pub const IOFlavour = enum {
    mmap,
    buffered,

    pub fn run(
        self: *const IOFlavour,
        w: *std.Io.Writer,
        comptime adlerType: AdlerType,
        path: []const u8,
    ) !void {
        return switch (self.*) {
            .mmap => try ioWithMmap(w, path, adlerType),
            .buffered => try ioWithBuffer(w, path, adlerType),
        };
    }
};

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

pub const Args = struct {
    pub const Positionals = PositionalOf(.{
        .TupleType = struct {
            IOFlavour,
            AdlerType,
            []const u8,
        },
        .ReminderType = void,
    });

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <iotype> <adlertype> <file>"},
        .description = "Cli to run hashing algorithms on a file treated as binary",
        .examples = &.{
            "flechette mmap adler32 random_1kb.bin",
            "flechette mmap adler64 random_250mb.bin",
            "flechette buffered adler32 random_32gb.bin",
            "flechette buffered adler64 random_250gb.bin",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "IOFlavour to use to read the binary",
                "Hashing algorith to use",
                "file path (relative)",
            },
        },
    };

    pub const HelpFmt = args.help.HelpFmt(
        Args,
        .{ .simpleTypes = true, .optionsBreakline = true },
    );
};

pub fn main() !u8 {
    var sfba = std.heap.stackFallback(4098, std.heap.page_allocator);
    const allocator = sfba.get();

    var buff: [1024]u8 = undefined;
    var stderrW = std.fs.File.stderr().writer(&buff);
    var w = &stderrW.interface;

    var res: SpecResponse(Args) = .init(allocator);
    res.parseArgs() catch |E| {
        try w.writeAll(Args.HelpFmt.helpForErr(@TypeOf(res).Error, E, "Failed with reason: "));
        try w.flush();
        return 1;
    };

    const ioType, const adlerType, const path = res.positionals.tuple;

    inline for (std.meta.fields(AdlerType)) |field| {
        if (std.mem.eql(u8, field.name, @tagName(adlerType))) {
            try ioType.run(w, @enumFromInt(field.value), path);
            break;
        }
    } else {
        @panic("Adler hash type not found");
    }

    return 0;
}

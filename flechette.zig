const std = @import("std");
const regent = @import("regent");
const zcasp = @import("zcasp");
const coll = regent.collections;
const spec = zcasp.spec;
const codec = zcasp.codec;
const positionals = zcasp.positionals;
const HelpData = zcasp.help.HelpData;
const Cursor = coll.Cursor;
const AsCursor = coll.AsCursor;
const adler = @import("flechette/adler.zig");
const Fadler = @import("flechette/Fadler.zig");

pub fn ioWithMmap(T: type, hasher: *T, argsRes: *const ArgsResponse) !void {
    const path = argsRes.positionals.tuple.@"1";

    var timer = try std.time.Timer.start();

    const f = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });
    const stat = try f.stat();
    const fileSize = stat.size;

    const ptr = try std.posix.mmap(
        null,
        fileSize,
        std.posix.PROT.READ,
        .{
            .TYPE = .PRIVATE,
            .NONBLOCK = true,
        },
        f.handle,
        0,
    );
    defer std.posix.munmap(ptr);

    var chunkTimer = try std.time.Timer.start();
    hasher.roll(ptr);
    const hasherElapsed = chunkTimer.read();

    try printReport(
        argsRes,
        hasher.hash,
        1,
        timer.read(),
        hasherElapsed,
        hasherElapsed,
        ptr.len,
        fileSize,
    );
}

pub fn ioHeap(T: type, hasher: *T, argsRes: *const ArgsResponse) !void {
    const path = argsRes.positionals.tuple.@"1";

    var totalTimer = try std.time.Timer.start();

    const f = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });
    const fstat = try f.stat();
    const fileSize = fstat.size;

    var chunkTimer = try std.time.Timer.start();
    const buff = try std.heap.page_allocator.alloc(u8, fileSize);
    defer std.heap.page_allocator.free(buff);
    const chunk = try f.readAll(buff);

    const ioElapsed = chunkTimer.read();

    chunkTimer.reset();
    hasher.roll(buff[0..chunk]);
    const hasherElapsed = chunkTimer.read();

    try printReport(
        argsRes,
        hasher.hash,
        1,
        totalTimer.read(),
        hasherElapsed,
        ioElapsed,
        fileSize,
        chunk,
    );
}

pub fn ioWithBuffer(T: type, hasher: *T, argsRes: *const ArgsResponse) !void {
    const path = argsRes.positionals.tuple.@"1";

    var totalTimer = try std.time.Timer.start();

    const f = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });

    // This does nothing for bigger files
    // heap pages doesnt seem to be bette either
    const buffLen = ByteUnit.mb * 2;
    var buff: [buffLen]u8 = undefined;

    const fstat = try f.stat();
    const fileSize = fstat.size;

    var reader = f.readerStreaming(&buff);

    var chunkIndex: usize = 0;
    var bytesProcessed: u64 = 0;

    var chunkTimer = try std.time.Timer.start();
    var hasherElapsed: u64 = 0;
    var ioElapsed: u64 = 0;
    while (true) {
        chunkTimer.reset();
        const slice = reader.interface.take(1) catch |e| switch (e) {
            error.EndOfStream => rv: {
                const eosChunk = reader.interface.buffered();
                if (eosChunk.len == 0) break;
                break :rv eosChunk;
            },
            error.ReadFailed => return e,
        };
        bytesProcessed += slice.len;
        chunkIndex += 1;
        ioElapsed += chunkTimer.read();

        chunkTimer.reset();
        hasher.roll(slice);
        hasherElapsed += chunkTimer.read();
    }
    const totalElapsed = totalTimer.read();

    try printReport(
        argsRes,
        hasher.hash,
        chunkIndex,
        totalElapsed,
        hasherElapsed,
        ioElapsed,
        fileSize,
        bytesProcessed,
    );
}

const NanoUnit = struct {
    pub const ms = 10e5;
    pub const s = 10e8;
};

const ByteUnit = struct {
    pub const mb = 1 << 20;
    pub const gb = 1 << 30;
};

fn printReport(
    argsRes: *const ArgsResponse,
    hash: anytype,
    chunkIndex: usize,
    totalElapsed: u64,
    hasherElapsed: u64,
    ioElapsed: u64,
    fileSize: u64,
    bytesProcessed: u64,
) !void {
    const upcase = argsRes.options.uppercase;
    var hexBuf: [@sizeOf(@TypeOf(hash)) * 8]u8 = undefined;
    var hexW: std.Io.Writer = .fixed(&hexBuf);
    try hexW.printInt(hash, 16, if (upcase) .upper else .lower, .{});
    const hex = hexW.buffered();
    try reporter.stdoutW.print("{s}\n", .{hex});

    const benchmark = argsRes.options.benchmark;
    const ioBenchmark = argsRes.options.@"io-benchmark";
    const totalElapsedF: f128 = @floatFromInt(totalElapsed);

    if (benchmark) {
        const elapsedF: f128 = @floatFromInt(hasherElapsed);
        const bytesProcessedF: f128 = @floatFromInt(bytesProcessed);
        const elapsedInSec = elapsedF / NanoUnit.s;
        try reporter.stderrW.print(
            "{s} hashing: 0x{s}, elapsed {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s\n",
            .{
                @tagName(argsRes.verb.?),
                hex,
                elapsedInSec,
                elapsedF / NanoUnit.ms,
                bytesProcessedF / ByteUnit.mb / elapsedInSec,
                bytesProcessedF / ByteUnit.gb / elapsedInSec,
            },
        );
    }

    if (ioBenchmark) {
        if (argsRes.positionals.tuple.@"0" == .mmap and benchmark) {
            try reporter.stderrW.writeAll("mmap io: benchmark skipped, mmap can't be benchmarked separately\n");
        } else {
            const elapsedF: f128 = @floatFromInt(ioElapsed);
            const bytesProcessedF: f128 = @floatFromInt(bytesProcessed);
            const elapsedInSec = elapsedF / NanoUnit.s;
            try reporter.stderrW.print(
                "{s} io: elapsed {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s, chunk {d} {d:.2}%\n",
                .{
                    @tagName(argsRes.positionals.tuple.@"0"),
                    elapsedInSec,
                    elapsedF / NanoUnit.ms,
                    bytesProcessedF / ByteUnit.mb / elapsedInSec,
                    bytesProcessedF / ByteUnit.gb / elapsedInSec,
                    chunkIndex,
                    bytesProcessedF / @as(f128, @floatFromInt(fileSize)) * 100.0,
                },
            );
        }
    }

    if (benchmark or ioBenchmark) {
        try reporter.stderrW.print(
            "total hashing elapsed: {d:.2}s {d:.2}ms\n",
            .{
                totalElapsedF / NanoUnit.s,
                totalElapsedF / NanoUnit.ms,
            },
        );
    }
}

pub const IOFlavour = enum {
    mmap,
    buffered,
    heap,

    pub fn run(
        self: *const IOFlavour,
        argsRes: *const ArgsResponse,
    ) !void {
        const VerbEnum = @typeInfo(Args.Verb).@"union".tag_type.?;
        const verb = argsRes.verb.?;

        inline for (std.meta.fields(VerbEnum), std.meta.fields(Args.Verb)) |eField, uField| {
            if (std.mem.eql(u8, uField.name, @tagName(verb))) {
                const HasherT = uField.type.HashT;
                var hasher: HasherT = switch (@as(VerbEnum, @enumFromInt(eField.value))) {
                    .adler32 => .{},
                    .adler64 => .{},
                    .fadler64 => .{
                        .flavour = verb.fadler64.positionals.tuple.@"0",
                    },
                };

                return switch (self.*) {
                    .mmap => try ioWithMmap(HasherT, &hasher, argsRes),
                    .buffered => try ioWithBuffer(HasherT, &hasher, argsRes),
                    .heap => try ioHeap(HasherT, &hasher, argsRes),
                };
            }
        } else {
            unreachable;
        }
    }
};

pub const PathCodec = struct {
    // NOTE: Oh no! A static buffer! We better never args parse twice!
    var pathBuff: [4098]u8 = undefined;

    pub const Error = error{
        MissingPath,
    } || std.fs.Dir.RealPathError || codec.PrimitiveCodec.Error;

    pub fn supports(comptime T: type, comptime _: anytype) bool {
        return T == []const u8;
    }

    pub fn parseByType(
        self: *@This(),
        comptime T: type,
        tag: anytype,
        allc: *const std.mem.Allocator,
        cursor: *Cursor([]const u8),
    ) Error!T {
        if (T == []const u8) {
            // NOTE: this takes like 30 micros
            const path = cursor.next() orelse return Error.MissingPath;
            return try std.fs.cwd().realpath(path, &pathBuff);
        } else {
            return codec.PrimitiveCodec.parseByType(self, T, tag, allc, cursor);
        }
    }
};

pub fn AdlerCmd(adlerType: adler.AdlerType) type {
    return struct {
        pub const HashT = adler.AdlerHash(adlerType);

        pub const Positionals = positionals.EmptyPositionalsOf;

        pub const Help: HelpData(@This()) = .{
            .usage = &.{"flechette <ioType> " ++ @tagName(adlerType) ++ " <file>"},
            .shortDescription = "Runs " ++ @tagName(adlerType) ++ " hashing algorithm on file",
            .description = "Runs " ++ @tagName(adlerType) ++ " hashing algorithm on file",
            .examples = &.{
                "flechette mmap " ++ @tagName(adlerType) ++ " r1gb.bin",
                "flechette buffered " ++ @tagName(adlerType) ++ " r1gb.bin",
            },
        };

        pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
            .ensureCursorDone = false,
        };
    };
}

pub const Fadler64Cmd = struct {
    pub const HashT = Fadler;

    pub const Positionals = positionals.PositionalOf(.{
        .TupleType = struct { Fadler.ExecutionFlavour },
        .ReminderType = void,
    });

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> fadler64 <executionFlavour> <file>"},
        .shortDescription = "Runs fadler64 hashing algorithm on file",
        .description = "Runs fadler64 hashing algorithm on file",
        .examples = &.{
            "flechette mmap fadler64 hdiff r1gb.bin",
            "flechette buffered fadler64 scalar16 r1gb.bin",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "Which fadler64 flavour to run. Supported values: " ++ zcasp.help.enumValueHint(Fadler.ExecutionFlavour),
            },
        },
    };

    pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
        .ensureCursorDone = false,
    };
};

pub const Args = struct {
    benchmark: bool = false,
    @"io-benchmark": bool = false,
    @"args-benchmark": bool = false,
    uppercase: bool = false,

    pub const Short = .{
        .b = .benchmark,
        .ib = .@"io-benchmark",
        .ab = .@"args-benchmark",
        .u = .uppercase,
    };

    pub const Positionals = positionals.PositionalOf(.{
        .CodecType = PathCodec,
        .TupleType = struct {
            IOFlavour,
            []const u8,
        },
        .ReminderType = void,
    });

    pub const Verb = union(enum) {
        adler32: AdlerCmd(.adler32),
        adler64: AdlerCmd(.adler64),
        fadler64: Fadler64Cmd,
    };

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> <command> <file>"},
        .description = "Cli to run hashing algorithms on a file treated as binary",
        .examples = &.{
            "Result only: flechette mmap adler64 r1gb.bin",
            "Benchmark hash: flechette -b buffered adler32 r1gb.bin",
            "Benchmark IO: flechette -ib buffered fadler64 scalar r1gb.bin",
            "TIP: run any command with --help and it will fail and show help",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "IOFlavour to use to read the binary. Supported values: " ++ zcasp.help.enumValueHint(IOFlavour),
                "file path (relative)",
            },
        },
        .optionsDescription = &.{
            .{ .field = .benchmark, .description = "Prints hash benchmark on stderr" },
            .{ .field = .@"io-benchmark", .description = "Prints io benchmark on stderr. Skipped if used with --benchmark and mmap" },
            .{ .field = .@"args-benchmark", .description = "Prints args parser benchmark on stderr" },
            .{ .field = .uppercase, .description = "Prints hash in uppercase hex" },
        },
    };

    pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
        .mandatoryVerb = true,
    };
};

const ArgsResponse = spec.SpecResponseWithConfig(Args, zcasp.help.HelpConf{
    .backwardsBranchesQuote = 1000000,
    .simpleTypes = true,
}, true);

const Reporter = struct {
    stdoutW: *std.Io.Writer = undefined,
    stderrW: *std.Io.Writer = undefined,
};

var reporter: *const Reporter = undefined;

pub fn main() !u8 {
    reporter = rv: {
        var r: Reporter = .{};
        var buffOut: [256]u8 = undefined;
        var buffErr: [4098]u8 = undefined;

        r.stdoutW = rOut: {
            var writer = std.fs.File.stdout().writer(&buffOut);
            break :rOut &writer.interface;
        };
        r.stderrW = rErr: {
            var writer = std.fs.File.stderr().writer(&buffErr);
            break :rErr &writer.interface;
        };

        break :rv &r;
    };

    var sfba = std.heap.stackFallback(4098, std.heap.page_allocator);
    const allocator = sfba.get();

    var timer = try std.time.Timer.start();
    var argsRes: ArgsResponse = .init(allocator);
    if (argsRes.parseArgs()) |parseError| {
        try reporter.stderrW.print("Last opt <{?s}>, Last token <{?s}>. ", .{ parseError.lastOpt, parseError.lastToken });
        try reporter.stderrW.writeAll(parseError.message orelse unreachable);
        try reporter.stderrW.flush();
        return 1;
    }

    if (argsRes.options.@"args-benchmark") {
        try reporter.stderrW.print(
            "args parser: elapsed {d:.2}ns\n",
            .{timer.read()},
        );
    }

    const ioFlavour: IOFlavour = argsRes.positionals.tuple.@"0";
    try ioFlavour.run(&argsRes);

    try reporter.stderrW.flush();
    try reporter.stdoutW.flush();

    return 0;
}

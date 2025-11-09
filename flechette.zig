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
const crc32 = @import("flechette/crc32.zig");
const Md5 = @import("flechette/md5.zig");
const byteUnit = zcasp.codec.byteUnit;
const units = regent.units;
const c = @import("flechette/c.zig").c;

pub fn dispatch(
    T: type,
    request: *HashRequest(T),
    result: *HashResult(T.R),
) !void {
    var totalTimer = try std.time.Timer.start();

    var hasher = request.hasher;
    var reader = request.reader;

    var chunkIndex: usize = 0;
    var bytesProcessed: u64 = 0;

    var chunkTimer = try std.time.Timer.start();
    var hasherElapsed: u64 = 0;
    var ioElapsed: u64 = 0;
    while (true) {
        chunkTimer.reset();
        const slice = reader.peekGreedy(1) catch |e| switch (e) {
            error.EndOfStream => rv: {
                const eosChunk = reader.buffered();
                if (eosChunk.len == 0) break;
                reader.tossBuffered();
                break :rv eosChunk;
            },
            error.ReadFailed => return e,
        };
        reader.tossBuffered();
        bytesProcessed += slice.len;
        chunkIndex += 1;
        ioElapsed += chunkTimer.read();

        chunkTimer.reset();
        hasher.roll(slice);
        hasherElapsed += chunkTimer.read();
    }
    const totalElapsed = totalTimer.read();

    result.* = .{
        .argsRes = result.argsRes,
        .hash = hasher.final(),
        .chunks = chunkIndex,
        .elapsed = totalElapsed,
        .hasherElapsed = hasherElapsed,
        .ioElapsed = ioElapsed,
        .fileSize = result.fileSize,
        .bytesProcessed = bytesProcessed,
    };

    try result.print();
}

pub const IOFlavourEnum = @typeInfo(IOFlavour).@"union".tag_type.?;

pub const StackBuffLen = enum {
    @"1b",
    @"2b",
    @"4b",
    @"8b",
    @"16b",
    @"32b",
    @"64b",
    @"128b",
    @"256b",
    @"512b",
    @"1kb",
    @"2kb",
    @"4kb",
    @"8kb",
    @"16kb",
    @"32kb",
    @"64kb",
    @"128kb",
    @"256kb",
    @"512kb",
    @"1mb",
    @"2mb",
    @"4mb",
    @"8mb",
};

pub fn HashRequest(HasherT: type) type {
    return struct {
        file: std.fs.File,
        reader: *std.Io.Reader,
        hasher: *HasherT,
    };
}

pub fn HashResult(T: type) type {
    return struct {
        argsRes: *const ArgsResponse,
        hash: T,
        chunks: usize,
        elapsed: u64,
        hasherElapsed: u64,
        ioElapsed: u64,
        fileSize: u64,
        bytesProcessed: u64,

        pub fn print(self: *@This()) !void {
            const upcase = self.argsRes.options.uppercase;
            const TInfo = @typeInfo(T);
            var hashStr: []const u8 = undefined;

            if (TInfo == .int) {
                var hexBuf: [@sizeOf(T) * 8]u8 = undefined;
                var hexW: std.Io.Writer = .fixed(&hexBuf);
                try hexW.printInt(self.hash, 16, if (upcase) .upper else .lower, .{});
                hashStr = hexW.buffered();
            } else if (TInfo == .array) {
                var hexBuf: [TInfo.array.len * 2]u8 = undefined;
                var hexW: std.Io.Writer = .fixed(&hexBuf);
                if (upcase)
                    try hexW.print("{X}", .{self.hash})
                else
                    try hexW.print("{x}", .{self.hash});

                hashStr = &hexBuf;
            } else @compileError("Invalid hash type: " ++ @typeName(T));

            var vecBuff: [2][]const u8 = .{
                hashStr,
                "\n",
            };
            try reporter.stdoutW.writeVecAll(&vecBuff);

            const benchmark = self.argsRes.options.benchmark;
            const ioBenchmark = self.argsRes.options.@"io-benchmark";
            const totalElapsedF: f128 = @floatFromInt(self.elapsed);

            if (benchmark) {
                const elapsedF: f128 = @floatFromInt(self.hasherElapsed);
                const bytesProcessedF: f128 = @floatFromInt(self.bytesProcessed);
                const elapsedInSec = elapsedF / units.NanoUnit.s;
                try reporter.stderrW.print(
                    "{s} hashing: 0x{s}, elapsed {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s\n",
                    .{
                        @tagName(self.argsRes.verb.?),
                        hashStr,
                        elapsedInSec,
                        elapsedF / units.NanoUnit.ms,
                        bytesProcessedF / units.ByteUnit.mb / elapsedInSec,
                        bytesProcessedF / units.ByteUnit.gb / elapsedInSec,
                    },
                );
            }

            if (ioBenchmark) {
                if (self.argsRes.positionals.tuple.@"0" == .mmap) {
                    try reporter.stderrW.writeAll("mmap io: benchmark skipped, mmap can't be benchmarked separately\n");
                } else {
                    const elapsedF: f128 = @floatFromInt(self.ioElapsed);
                    const bytesProcessedF: f128 = @floatFromInt(self.bytesProcessed);
                    const elapsedInSec = elapsedF / units.NanoUnit.s;
                    try reporter.stderrW.print(
                        "{s} io: elapsed {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s, chunk {d} {d:.2}%\n",
                        .{
                            @tagName(self.argsRes.positionals.tuple.@"0"),
                            elapsedInSec,
                            elapsedF / units.NanoUnit.ms,
                            bytesProcessedF / units.ByteUnit.mb / elapsedInSec,
                            bytesProcessedF / units.ByteUnit.gb / elapsedInSec,
                            self.chunks,
                            bytesProcessedF / @as(f128, @floatFromInt(self.fileSize)) * 100.0,
                        },
                    );
                }
            }

            if (benchmark or ioBenchmark) {
                try reporter.stderrW.print(
                    "total hashing elapsed: {d:.2}s {d:.2}ms\n",
                    .{
                        totalElapsedF / units.NanoUnit.s,
                        totalElapsedF / units.NanoUnit.ms,
                    },
                );
            }
        }
    };
}

pub const IOFlavour = union(enum) {
    mmap,
    stack: StackBuffLen,
    heap: byteUnit.ByteUnit,
    direct,

    const Error = error{
        InvalidStackSize,
    };

    pub fn run(
        self: *const IOFlavour,
        argsRes: *const ArgsResponse,
    ) !void {
        const VerbEnum = @typeInfo(Args.Verb).@"union".tag_type.?;
        const verb = argsRes.verb.?;
        // This likely wont work on older systems, seele has the same problem and I still dont have a good solution
        // for this
        var stackAllocBuffer: [units.ByteUnit.mb * 8]u8 = undefined;
        var sba = std.heap.FixedBufferAllocator.init(&stackAllocBuffer);
        const stackAllocator = sba.allocator();
        const heapAllocator = std.heap.page_allocator;

        const path = argsRes.positionals.tuple.@"1";

        inline for (std.meta.fields(VerbEnum), std.meta.fields(Args.Verb)) |eField, uField| {
            if (std.mem.eql(u8, uField.name, @tagName(verb))) {
                const HasherT = uField.type.HashT;
                var hasher: HasherT = switch (@as(VerbEnum, @enumFromInt(eField.value))) {
                    .md5 => .init(),
                    .fadler64 => .{
                        .flavour = verb.fadler64.positionals.tuple.@"0",
                    },
                    inline else => .{},
                };

                var request: HashRequest(HasherT) = undefined;
                var result: HashResult(HasherT.R) = undefined;

                result.argsRes = argsRes;

                switch (self.*) {
                    .mmap => {
                        const file = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });
                        defer file.close();

                        const stat = try file.stat();
                        const fileSize = stat.size;
                        result.fileSize = fileSize;

                        const ptr = try std.posix.mmap(
                            null,
                            fileSize,
                            std.posix.PROT.READ,
                            .{
                                .TYPE = .PRIVATE,
                                .NONBLOCK = true,
                                .POPULATE = true,
                            },
                            file.handle,
                            0,
                        );
                        defer std.posix.munmap(ptr);

                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_SEQUENTIAL,
                        );
                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_NOREUSE,
                        );

                        var reader: std.Io.Reader = .fixed(ptr);

                        request = .{
                            .file = file,
                            .reader = &reader,
                            .hasher = &hasher,
                        };

                        return try dispatch(HasherT, &request, &result);
                    },
                    // NOTE:
                    // I tried readahead but it did nothing, fadv_noreuse seems to be the only thing
                    // speeding up stack-based buffer io
                    // O_DIRECT can't be used due to alignment issues
                    .stack => |tagSize| {
                        inline for (std.meta.fields(StackBuffLen)) |field| {
                            if (std.mem.eql(u8, field.name, @tagName(tagSize))) {
                                @setEvalBranchQuota(1000 * 10);
                                const bUnit = comptime switch (@as(StackBuffLen, @enumFromInt(field.value))) {
                                    // NOTE: this is a bit wasteful but only at comptime
                                    inline else => |tag| byteUnit.parse(@tagName(tag)) catch @compileError(
                                        std.fmt.comptimePrint("This failed: {s}\n", .{field.name}),
                                    ),
                                };

                                const buff = try stackAllocator.alloc(u8, bUnit.size());
                                defer stackAllocator.free(buff);

                                const file = try std.fs.openFileAbsolute(path, .{ .mode = .read_only });
                                defer file.close();

                                const stat = try file.stat();
                                const fileSize = stat.size;
                                result.fileSize = fileSize;
                                _ = std.os.linux.fadvise(
                                    file.handle,
                                    0,
                                    @bitCast(fileSize),
                                    c.POSIX_FADV_SEQUENTIAL,
                                );
                                _ = std.os.linux.fadvise(
                                    file.handle,
                                    0,
                                    @bitCast(fileSize),
                                    c.POSIX_FADV_NOREUSE,
                                );

                                var fReader = file.reader(buff);
                                request = .{
                                    .file = file,
                                    .reader = &fReader.interface,
                                    .hasher = &hasher,
                                };

                                return try dispatch(HasherT, &request, &result);
                            }
                        } else {
                            return Error.InvalidStackSize;
                        }
                    },
                    .heap => |bufferSize| {
                        const buff = try heapAllocator.alloc(u8, bufferSize.size());
                        defer heapAllocator.free(buff);

                        const file: std.fs.File = .{
                            .handle = try std.posix.open(path, .{ .DIRECT = true }, 0),
                        };
                        defer file.close();

                        const stat = try file.stat();
                        const fileSize = stat.size;
                        result.fileSize = fileSize;
                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_SEQUENTIAL,
                        );
                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_DONTNEED,
                        );

                        var fReader = file.reader(buff);
                        fReader.size = fileSize;

                        request = .{
                            .file = file,
                            .reader = &fReader.interface,
                            .hasher = &hasher,
                        };

                        return try dispatch(HasherT, &request, &result);
                    },
                    .direct => {
                        const file: std.fs.File = .{
                            .handle = try std.posix.open(path, .{ .DIRECT = true }, 0),
                        };
                        defer file.close();

                        const stat = try file.stat();
                        const fileSize = stat.size;
                        result.fileSize = fileSize;
                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_SEQUENTIAL,
                        );
                        _ = std.os.linux.fadvise(
                            file.handle,
                            0,
                            @bitCast(fileSize),
                            c.POSIX_FADV_DONTNEED,
                        );

                        const buff = try heapAllocator.alloc(u8, fileSize);
                        defer heapAllocator.free(buff);

                        var fReader = file.reader(buff);

                        request = .{
                            .file = file,
                            .reader = &fReader.interface,
                            .hasher = &hasher,
                        };

                        return try dispatch(HasherT, &request, &result);
                    },
                }
            }
        } else {
            unreachable;
        }
    }
};

pub const PosCodec = struct {
    pub const Error = error{
        MissingPath,
        MissingIOFlavourEnumArg,
        ZeroSize,
    } || std.fs.Dir.RealPathError ||
        codec.PrimitiveCodec.Error ||
        byteUnit.Error;

    pub fn supports(comptime T: type, comptime _: anytype) bool {
        return T == []const u8 or T == IOFlavour;
    }

    pub fn parseByType(
        self: *@This(),
        comptime T: type,
        tag: anytype,
        allocator: *const std.mem.Allocator,
        cursor: *Cursor([]const u8),
    ) Error!T {
        if (T == []const u8) {
            const path = cursor.next() orelse return Error.MissingPath;
            if (std.fs.path.isAbsolute(path)) return path;

            const pathBuff = try allocator.alloc(u8, 4096);
            return try std.fs.cwd().realpath(path, pathBuff);
        } else if (T == IOFlavour) {
            const enTag = try codec.PrimitiveCodec.parseByType(self, IOFlavourEnum, .null, allocator, cursor);
            inline for (@typeInfo(IOFlavourEnum).@"enum".fields) |field| {
                if (std.mem.eql(u8, field.name, @tagName(enTag))) {
                    const name = comptime field.name;
                    const TagT = @FieldType(IOFlavour, name);

                    if (TagT == void) return @unionInit(IOFlavour, name, {});
                    if (TagT == StackBuffLen) {
                        return @unionInit(IOFlavour, name, try codec.PrimitiveCodec.parseByType(
                            self,
                            TagT,
                            .null,
                            allocator,
                            cursor,
                        ));
                    }
                    if (TagT == byteUnit.ByteUnit) {
                        const sizeStr = cursor.next() orelse return Error.MissingPath;
                        const bUnit = try zcasp.codec.byteUnit.parse(sizeStr);
                        if (bUnit.size() == 0) return Error.ZeroSize;
                        return @unionInit(IOFlavour, name, bUnit);
                    }

                    @compileError(
                        "Unimplemented IOFlavour active value type parse: " ++ @typeName(TagT),
                    );
                }
            } else {
                unreachable;
            }
        } else {
            return try codec.PrimitiveCodec.parseByType(self, T, tag, allocator, cursor);
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
                "flechette direct " ++ @tagName(adlerType) ++ " r1gb.bin",
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
            "flechette heap 8mb fadler64 scalar16 r1gb.bin",
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

pub const Crc32Cmd = struct {
    pub const HashT = crc32.Wrapper;

    pub const Positionals = positionals.EmptyPositionalsOf;

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> crc32 <file>"},
        .shortDescription = "Runs crc32 hashing algorithm on file",
        .description = "Runs crc32 hashing algorithm on file",
        .examples = &.{
            "flechette mmap crc32 r1gb.bin",
        },
    };

    pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
        .ensureCursorDone = false,
    };
};

pub const Md5Cmd = struct {
    pub const HashT = Md5;

    pub const Positionals = positionals.EmptyPositionalsOf;

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> md5 <file>"},
        .shortDescription = "Runs md5 hashing algorithm on file",
        .description = "Runs md5 hashing algorithm on file",
        .examples = &.{
            "flechette mmap md5 r1gb.bin",
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
        .CodecType = PosCodec,
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
        crc32: Crc32Cmd,
        md5: Md5Cmd,
    };

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> <command> <file>"},
        .description = "Cli to run hashing algorithms on a file treated as binary",
        .examples = &.{
            "Result only: flechette mmap adler64 r1gb.bin",
            "Benchmark hash: flechette -b heap 8mb adler32 r1gb.bin",
            "Benchmark IO: flechette -ib stack 4mb fadler64 scalar r1gb.bin",
            "TIP: run any command with --help and it will fail and show help",
        },
        .positionalsDescription = .{
            .tuple = &.{
                regent.collections.ComptSb.initTup(.{
                    "IOFlavour to use to read the binary. Supported values: ",
                    zcasp.help.enumValueHint(IOFlavourEnum),
                    ". stack and heap need an extra positional argument with the size and optional unit. Example: 1mb, 256kb. Currently using heap with a good buffer for your driver is the best approach. It will use O_DIRECT. Stack/mmap seems to do better for smaller files.",
                }).s,
                "File path (can be relative)",
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
            var writer = std.fs.File.stdout().writerStreaming(&buffOut);
            break :rOut &writer.interface;
        };
        r.stderrW = rErr: {
            var writer = std.fs.File.stderr().writerStreaming(&buffErr);
            break :rErr &writer.interface;
        };

        break :rv &r;
    };

    var stackAllocBuffer: [units.ByteUnit.kb * 8]u8 = undefined;
    var sba = std.heap.FixedBufferAllocator.init(&stackAllocBuffer);
    const stackAllocator = sba.allocator();

    var timer = try std.time.Timer.start();
    var argsRes: ArgsResponse = .init(stackAllocator);
    defer argsRes.deinit();

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

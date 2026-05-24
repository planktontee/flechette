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
const xxh3 = @import("flechette/xxh3.zig");
const aegis = @import("flechette/aegis.zig");
const rapidhash = @import("flechette/rapidhash.zig");
const byteUnit = zcasp.codec.byteUnit;
const units = regent.units;
const fs = regent.fs;
const c = @import("flechette/c.zig").c;
const openssl = @import("flechette/openssl.zig");

pub fn dispatch(
    T: type,
    request: *HashRequest(T),
    result: *HashResult(T.R),
) !void {
    var totalTimer = std.Io.Clock.awake.now(io);

    var hasher = request.hasher;
    var reader = request.reader;

    var chunkIndex: usize = 0;
    var bytesProcessed: u64 = 0;

    var chunkTimer: std.Io.Timestamp = undefined;
    var hasherElapsed: u64 = 0;
    var ioElapsed: u64 = 0;
    while (true) {
        chunkTimer = std.Io.Clock.awake.now(io);
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
        ioElapsed += @intCast(chunkTimer.untilNow(io, .awake).toNanoseconds());

        chunkTimer = std.Io.Clock.awake.now(io);
        hasher.roll(slice);
        hasherElapsed += @intCast(chunkTimer.untilNow(io, .awake).toNanoseconds());
    }
    const totalElapsed: u64 = @intCast(totalTimer.untilNow(io, .awake).toNanoseconds());

    result.* = .{
        .argsRes = result.argsRes,
        .path = result.path,
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

pub fn HashRequest(HasherT: type) type {
    return struct {
        file: std.Io.File,
        reader: *std.Io.Reader,
        hasher: *HasherT,
    };
}

pub fn HashResult(T: type) type {
    return struct {
        argsRes: *const ArgsResponse,
        path: [2][]const u8,
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

            try reporter.stdoutW.writeAll(hashStr);

            if (self.argsRes.options.name) {
                var vecBuff: [5][]const u8 = .{
                    "    ",
                    self.path[0],
                    if (self.path[1].len == 0) "" else "/",
                    self.path[1],
                    "\n",
                };
                try reporter.stdoutW.writeVecAll(&vecBuff);
            } else {
                try reporter.stdoutW.writeAll("\n");
            }

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

            try reporter.stderrW.flush();
            try reporter.stdoutW.flush();
        }
    };
}

pub const IOFlavour = union(enum) {
    mmap,
    stack: ?byteUnit.ByteUnit,
    heap: ?byteUnit.ByteUnit,
    direct: ?byteUnit.ByteUnit,

    const Error = error{
        InvalidStackSize,
        DirectHasNoTargetFile,
        MmapHasNoTargetFile,
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
        const stackContext: regent.ergo.Context = .{
            .io = io,
            .allocator = stackAllocator,
        };

        const pageContext: regent.ergo.Context = .{
            .io = io,
            .allocator = std.heap.page_allocator,
        };

        const mmapContext: regent.ergo.Context = .{
            .io = io,
            .allocator = stackAllocator,
        };

        inline for (std.meta.fields(VerbEnum), std.meta.fields(Args.Verb)) |verbEField, verbUField| {
            if (std.mem.eql(u8, verbUField.name, @tagName(verb))) {
                const HasherT = verbUField.type.HashT;
                const paths: []const []const u8 = if (argsRes.positionals.reminder) |reminder| reminder else &.{"-"};

                const context = switch (self.*) {
                    .mmap => mmapContext,
                    .stack => stackContext,
                    .heap, .direct => pageContext,
                };

                const openConfig: fs.OpenConfig = switch (self.*) {
                    .mmap, .stack, .heap => .{},
                    .direct => .{ .oDirect = true },
                };

                const bufferConfig: fs.BufferConfig = switch (self.*) {
                    .mmap => fs.defaultBufferConfig(.read),
                    inline else => |bUnit| if (bUnit) |b| .initSame(b.size()) else fs.defaultBufferConfig(.read),
                };

                const bufferType: fs.BufferType = switch (self.*) {
                    .mmap => .mmap,
                    inline else => |bUnit| if (bUnit != null) .byte else .full,
                };

                var fileCursor = regent.fs.FileCursor(.read).init(paths, argsRes.options.recursive);

                while (true) {
                    if (fileCursor.nextWithConfig(
                        context,
                        openConfig,
                        bufferType,
                        bufferConfig,
                    )) |optStream| {
                        if (optStream == null) break;

                        const path = fileCursor.lastPath().?;
                        var fstream = optStream.?;
                        defer fstream.close(context);

                        if (fstream.stream.size) |size| {
                            fstream.fadvise(context, 0, size, &.{
                                regent.linux.FADVISE.SEQUENTIAL,
                                regent.linux.FADVISE.NOREUSE,
                            });
                        }

                        var hasher: HasherT = switch (@as(VerbEnum, @enumFromInt(verbEField.value))) {
                            .md5, .sha256 => .init(),
                            .fadler64 => .{ .flavour = verb.fadler64.positionals.tuple.@"0" },
                            inline else => .{},
                        };

                        var request: HashRequest(HasherT) = .{
                            .file = fstream.stream.file,
                            .reader = &fstream.stream.interface,
                            .hasher = &hasher,
                        };
                        var result: HashResult(HasherT.R) = undefined;
                        result.argsRes = argsRes;
                        result.path = path;

                        dispatch(HasherT, &request, &result) catch |err| {
                            try reporter.stdoutW.print("{s}{s}Could not hash file - {s}{s}{s}\n", .{
                                @errorName(err),
                                "    ",
                                path[0],
                                if (path[1].len == 0) "" else "/",
                                path[1],
                            });
                            try reporter.stdoutW.flush();
                        };
                    } else |err| {
                        const path = fileCursor.lastPath().?;

                        try reporter.stdoutW.print("{s}{s}Could not open file - {s}{s}{s}\n", .{
                            @errorName(err),
                            "    ",
                            path[0],
                            if (path[1].len == 0) "" else "/",
                            path[1],
                        });
                        try reporter.stdoutW.flush();
                    }
                }
                return;
            }
        } else unreachable;
    }
};

pub const IOFlavourEnum = @typeInfo(IOFlavour).@"union".tag_type.?;

pub const PosCodec = struct {
    pub const Error = error{
        MissingPath,
        MissingIOFlavourEnumArg,
        ZeroSize,
    } || std.Io.Dir.RealPathError ||
        codec.PrimitiveCodec.Error ||
        byteUnit.Error;

    pub fn supports(comptime T: type, comptime _: anytype) bool {
        return T == IOFlavour;
    }

    pub fn parseByType(
        self: *@This(),
        comptime T: type,
        tag: anytype,
        allocator: *const std.mem.Allocator,
        cursor: *Cursor([]const u8),
    ) Error!T {
        if (T == IOFlavour) {
            const enTag = try codec.PrimitiveCodec.parseByType(self, IOFlavourEnum, .null, allocator, cursor);
            inline for (@typeInfo(IOFlavourEnum).@"enum".fields) |field| {
                if (std.mem.eql(u8, field.name, @tagName(enTag))) {
                    const name = comptime field.name;
                    const TagT = @FieldType(IOFlavour, name);

                    if (TagT == void) return @unionInit(IOFlavour, name, {});
                    if (TagT == ?byteUnit.ByteUnit) {
                        var tagValue: ?byteUnit.ByteUnit = null;

                        if (cursor.peek()) |sizeStr| {
                            if (zcasp.codec.byteUnit.parse(sizeStr)) |bUnit| {
                                cursor.consume();
                                if (bUnit.size() == 0) return Error.ZeroSize;
                                tagValue = bUnit;
                            } else |_| {}
                        }

                        return @unionInit(IOFlavour, name, tagValue);
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
            .usage = &.{"flechette <ioFlavour> " ++ @tagName(adlerType) ++ " <file>"},
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
        .usage = &.{"flechette <ioFlavour> fadler64 <executionFlavour> <file>"},
        .shortDescription = "Runs fadler64 hashing algorithm on file",
        .description = "Runs fadler64 hashing algorithm on file",
        .examples = &.{
            "flechette mmap fadler64 hdiff r1gb.bin",
            "flechette heap fadler64 scalar16 r1gb.bin",
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

pub fn BasicHashingCmd(_HashT: type, comptime name: []const u8) type {
    return struct {
        pub const HashT = _HashT;

        pub const Positionals = positionals.EmptyPositionalsOf;

        pub const Help: HelpData(@This()) = .{
            .usage = &.{"flechette <ioFlavour> " ++ name ++ " <file1 ... fileN>"},
            .shortDescription = "Runs " ++ name ++ " hashing algorithm on file",
            .description = "Runs " ++ name ++ " hashing algorithm on file",
            .examples = &.{
                "flechette mmap " ++ name ++ " r1gb.bin",
            },
        };

        pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
            .ensureCursorDone = false,
        };
    };
}

pub const Args = struct {
    benchmark: bool = false,
    @"io-benchmark": bool = false,
    @"args-benchmark": bool = false,
    uppercase: bool = false,
    name: bool = false,
    recursive: bool = false,

    pub const Short = .{
        .b = .benchmark,
        .ib = .@"io-benchmark",
        .ab = .@"args-benchmark",
        .u = .uppercase,
        .n = .name,
        .R = .recursive,
    };

    pub const Positionals = positionals.PositionalOf(.{
        .CodecType = PosCodec,
        .TupleType = struct {
            IOFlavour,
        },
        .ReminderType = ?[]const []const u8,
    });

    pub const Verb = union(enum) {
        adler32: AdlerCmd(.adler32),
        adler64: AdlerCmd(.adler64),
        fadler64: Fadler64Cmd,
        crc32: BasicHashingCmd(crc32.Wrapper, "crc32"),
        xxh3: BasicHashingCmd(xxh3, "xxh3"),
        aegis128: BasicHashingCmd(aegis, "aegis"),
        rapidhash: BasicHashingCmd(rapidhash, "rapidhash"),
        md5: BasicHashingCmd(openssl.MD5, "md5"),
        sha256: BasicHashingCmd(openssl.SHA256, "sha256"),
    };

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioType> <command> <file1 ... fileN>"},
        .description = "Cli to run hashing algorithms on a file treated as binary",
        .examples = &.{
            "Result only: flechette mmap adler64 r1gb.bin",
            "Benchmark hash: flechette -b heap xxh32 r1gb.bin",
            "Benchmark IO: flechette -ib stack md5 r1gb.bin",
            "TIP: run any command with --help and it will fail and show help",
        },
        .positionalsDescription = .{
            .tuple = &.{
                regent.collections.ComptSb.initTup(.{
                    "IOFlavour to use to read the binary. Supported values: ",
                    zcasp.help.enumValueHint(IOFlavourEnum),
                    ". stack and heap need an extra positional argument with the size and optional unit. Example: 1mb, 256kb. Currently using heap with a good buffer for your driver is the best approach. It will use O_DIRECT. Stack/mmap seems to do better for smaller files.",
                }).s,
            },
            .reminder = "File to hash, if nothing provided uses stdin.",
        },
        .optionsDescription = &.{
            .{ .field = .benchmark, .description = "Prints hash benchmark on stderr" },
            .{ .field = .@"io-benchmark", .description = "Prints io benchmark on stderr. Skipped if used with --benchmark and mmap" },
            .{ .field = .@"args-benchmark", .description = "Prints args parser benchmark on stderr" },
            .{ .field = .uppercase, .description = "Prints hash in uppercase hex" },
            .{ .field = .name, .description = "Prints path" },
            .{ .field = .recursive, .description = "Will recursively follow directories and symlinks" },
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
var io: std.Io = undefined;

pub fn main(init: std.process.Init.Minimal) !u8 {
    io = v: {
        var i = std.Io.Threaded.init_single_threaded;
        break :v i.io();
    };

    reporter = rv: {
        var r: Reporter = .{};
        var buffOut: [256]u8 = undefined;
        var buffErr: [4098]u8 = undefined;

        r.stdoutW = rOut: {
            var writer = std.Io.File.stdout().writerStreaming(io, &buffOut);
            break :rOut &writer.interface;
        };
        r.stderrW = rErr: {
            var writer = std.Io.File.stderr().writerStreaming(io, &buffErr);
            break :rErr &writer.interface;
        };

        break :rv &r;
    };

    var stackAllocBuffer: [units.ByteUnit.kb * 8]u8 = undefined;
    var sba = std.heap.FixedBufferAllocator.init(&stackAllocBuffer);
    const stackAllocator = sba.allocator();

    var clock = std.Io.Clock.awake.now(io);
    var argsRes: ArgsResponse = .init(stackAllocator);
    defer argsRes.deinit();

    if (argsRes.parseArgs(init.args)) |parseError| {
        try reporter.stderrW.print("Last opt <{?s}>, Last token <{?s}>. ", .{ parseError.lastOpt, parseError.lastToken });
        try reporter.stderrW.writeAll(parseError.message orelse unreachable);
        try reporter.stderrW.flush();
        return 1;
    }

    if (argsRes.options.@"args-benchmark") {
        try reporter.stderrW.print(
            "args parser: elapsed {d:.2}ns\n",
            .{clock.untilNow(io, .awake).toNanoseconds()},
        );
    }

    const ioFlavour: IOFlavour = argsRes.positionals.tuple.@"0";
    try ioFlavour.run(&argsRes);

    try reporter.stderrW.flush();
    try reporter.stdoutW.flush();

    return 0;
}

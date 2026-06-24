const std = @import("std");
const builtin = @import("builtin");
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
const Blake2b = @import("flechette/blake2b.zig");
const byteUnit = zcasp.codec.byteUnit;
const units = regent.units;
const fs = regent.fs;
const c = @import("flechette/c.zig").c;
const openssl = @import("flechette/openssl.zig");

pub fn dispatch(
    T: type,
    ctx: *Ctx,
    request: *HashRequest(T),
    result: *HashResult(T.R),
) !void {
    var totalTimer = std.Io.Clock.awake.now(ctx.io);

    var hasher = request.hasher;
    var reader = request.reader;

    var chunkIndex: usize = 0;
    var bytesProcessed: u64 = 0;

    var chunkTimer: std.Io.Timestamp = undefined;
    var hasherElapsed: u64 = 0;
    var ioElapsed: u64 = 0;
    while (true) {
        chunkTimer = std.Io.Clock.awake.now(ctx.io);
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
        ioElapsed += @intCast(chunkTimer.untilNow(ctx.io, .awake).toNanoseconds());

        chunkTimer = std.Io.Clock.awake.now(ctx.io);
        hasher.roll(slice);
        hasherElapsed += @intCast(chunkTimer.untilNow(ctx.io, .awake).toNanoseconds());
    }
    const totalElapsed: u64 = @intCast(totalTimer.untilNow(ctx.io, .awake).toNanoseconds());

    try result.print(
        ctx,
        hasher.final(),
        chunkIndex,
        totalElapsed,
        hasherElapsed,
        ioElapsed,
        bytesProcessed,
    );
}

pub fn HashRequest(HasherT: type) type {
    return struct {
        file: std.Io.File,
        reader: *std.Io.Reader,
        hasher: *HasherT,
    };
}

pub fn HashResult(T: type) type {
    const RInfo = @typeInfo(T);
    const RisSlice = RInfo == .pointer and RInfo.pointer.size == .slice;
    return struct {
        context: regent.ergo.Context,
        argsRes: *const ArgsResponse,
        path: []const u8,
        fileSize: ?u64,
        auxBuff: if (RisSlice) ?[]u8 else void = if (RisSlice) null else {},

        pub fn print(
            self: *@This(),
            ctx: *Ctx,
            hash: T,
            chunks: usize,
            elapsed: u64,
            hasherElapsed: u64,
            ioElapsed: u64,
            bytesProcessed: u64,
        ) !void {
            const upcase = self.argsRes.options.uppercase;
            const TInfo = @typeInfo(T);
            var hashStr: []const u8 = undefined;

            if (TInfo == .int) {
                var hexBuf: [@sizeOf(T) * 8]u8 = undefined;
                var hexW: std.Io.Writer = .fixed(&hexBuf);
                try hexW.printInt(hash, 16, if (upcase) .upper else .lower, .{});
                hashStr = hexW.buffered();
            } else if (TInfo == .array) {
                var hexBuf: [TInfo.array.len * 2]u8 = undefined;
                var hexW: std.Io.Writer = .fixed(&hexBuf);
                if (upcase)
                    try hexW.print("{X}", .{hash})
                else
                    try hexW.print("{x}", .{hash});

                hashStr = &hexBuf;
            } else if (RisSlice) {
                const hexBuf = r: {
                    if (self.auxBuff) |hexBuff| {
                        @memset(hexBuff, undefined);
                        break :r hexBuff;
                    } else {
                        self.auxBuff = try self.context.allocator.alloc(u8, hash.len * 2);
                        break :r self.auxBuff.?;
                    }
                };
                var hexW: std.Io.Writer = .fixed(hexBuf);
                if (upcase)
                    try hexW.print("{X}", .{hash})
                else
                    try hexW.print("{x}", .{hash});
                hashStr = hexBuf;
            } else @compileError("Invalid hash type: " ++ @typeName(T));
            defer if (TInfo == .pointer) self.context.allocator.free(hashStr);

            // NOTE: this is actually insanely wasteful before IO happens after a lot of computations
            try ctx.reporter.mutex.lock(ctx.io);
            defer ctx.reporter.mutex.unlock(ctx.io);

            try ctx.reporter.stdoutW.writeAll(hashStr);

            if (self.argsRes.options.name or self.argsRes.options.recursive or self.argsRes.options.@"recursive-follow-symlink") {
                var vecBuff: [3][]const u8 = .{
                    "    ",
                    self.path,
                    "\n",
                };
                try ctx.reporter.stdoutW.writeVecAll(&vecBuff);
            } else {
                try ctx.reporter.stdoutW.writeAll("\n");
            }

            const benchmark = self.argsRes.options.benchmark;
            const ioBenchmark = self.argsRes.options.@"io-benchmark";
            const totalElapsedF: f128 = @floatFromInt(elapsed);

            if (benchmark) {
                const elapsedF: f128 = @floatFromInt(hasherElapsed);
                const bytesProcessedF: f128 = @floatFromInt(bytesProcessed);
                const elapsedInSec = elapsedF / units.NanoUnit.s;
                try ctx.reporter.stderrW.print(
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
                    try ctx.reporter.stderrW.writeAll("mmap io: benchmark skipped, mmap can't be benchmarked separately\n");
                } else {
                    const elapsedF: f128 = @floatFromInt(ioElapsed);
                    const bytesProcessedF: f128 = @floatFromInt(bytesProcessed);
                    const elapsedInSec = elapsedF / units.NanoUnit.s;
                    try ctx.reporter.stderrW.print(
                        "{s} io: elapsed {d:.2}s {d:.2}ms, {d:.2} MB/s {d:.2} GB/s, chunk {d} {d:.2}%\n",
                        .{
                            @tagName(self.argsRes.positionals.tuple.@"0"),
                            elapsedInSec,
                            elapsedF / units.NanoUnit.ms,
                            bytesProcessedF / units.ByteUnit.mb / elapsedInSec,
                            bytesProcessedF / units.ByteUnit.gb / elapsedInSec,
                            chunks,
                            bytesProcessedF / @as(f128, @floatFromInt(self.fileSize orelse bytesProcessed)) * 100.0,
                        },
                    );
                }
            }

            if (benchmark or ioBenchmark) {
                try ctx.reporter.stderrW.print(
                    "total hashing elapsed: {d:.2}s {d:.2}ms\n",
                    .{
                        totalElapsedF / units.NanoUnit.s,
                        totalElapsedF / units.NanoUnit.ms,
                    },
                );
            }

            if (ctx.reporter.errIsTTY() and (benchmark or ioBenchmark)) try ctx.reporter.stderrW.flush();
            if (ctx.reporter.outIsTTY()) try ctx.reporter.stdoutW.flush();
        }

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (RisSlice) {
                if (self.auxBuff) |buff| allocator.free(buff);
            }
        }
    };
}

pub const IOFlavour = union(enum) {
    mmap,
    promoter: ?byteUnit.ByteUnit,
    stack: ?byteUnit.ByteUnit,
    heap: ?byteUnit.ByteUnit,
    direct: ?byteUnit.ByteUnit,

    const Error = error{
        InvalidStackSize,
        DirectHasNoTargetFile,
        MmapHasNoTargetFile,
    };

    // this uses a return param because it may fail itself
    fn handleError(ctx: *Ctx, path: []const u8, err: anyerror, failed: *bool) !void {
        failed.* |= true;
        try ctx.reporter.mutex.lock(ctx.io);
        defer ctx.reporter.mutex.unlock(ctx.io);

        try ctx.reporter.stdoutW.print("{s}    Could not hash file - {s}\n", .{
            @errorName(err),
            path,
        });
        if (ctx.reporter.outIsTTY()) try ctx.reporter.stdoutW.flush();
    }

    pub fn run(
        self: *const IOFlavour,
        argsRes: *const ArgsResponse,
        ctx: *Ctx,
    ) !bool {
        const VerbEnum = @typeInfo(Args.Verb).@"union".tag_type.?;
        const verb = argsRes.verb.?;

        // TODO: this will use the main thread's stack memory, which is awful for core locality
        // we need to trampoline from the actual thread, shit will be rad lol
        const stackContext: regent.ergo.Context = .{
            .io = ctx.io,
            .allocator = ctx.stackAllocator,
        };

        const pageContext: regent.ergo.Context = .{
            .io = ctx.io,
            .allocator = if (builtin.mode == .Debug) ctx.debugAllocator.allocator() else ctx.heapAllocator,
        };

        const mmapContext: regent.ergo.Context = .{
            .io = ctx.io,
            // this is used for small allocations like WeakRef set for pathings etc
            .allocator = ctx.stackAllocator,
        };

        const promoterContext: regent.ergo.Context = .{
            .io = ctx.io,
            .allocator = if (builtin.mode == .Debug)
                ctx.debugAllocator.allocator()
            else r: {
                // NOTE: promoter is just straightup broken in threaded even with fba threadSafe, so Promoting needs to be adapted
                const fba: *std.heap.FixedBufferAllocator = @ptrCast(@alignCast(ctx.stackAllocator.ptr));
                var promotingFba: regent.mem.PromotingSfba = .{
                    .fixed_buffer_allocator = fba.*,
                    .fallback_allocator = ctx.heapAllocator,
                };
                const allc = promotingFba.allocator();
                break :r allc;
            },
        };

        inline for (std.meta.fields(VerbEnum), std.meta.fields(Args.Verb)) |verbEField, verbUField| {
            if (std.mem.eql(u8, verbUField.name, @tagName(verb))) {
                const HasherT = verbUField.type.HashT;

                const context = switch (self.*) {
                    .mmap => mmapContext,
                    .stack => stackContext,
                    .heap, .direct => pageContext,
                    .promoter => promoterContext,
                };

                const openConfig: fs.OpenConfig = switch (self.*) {
                    .mmap, .stack, .heap, .promoter => .{},
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

                var result: HashResult(HasherT.R) = undefined;
                defer result.deinit(context.allocator);
                result.context = context;
                result.argsRes = argsRes;

                const alignment = regent.fs.oDirectAlignment;
                var resizeableBuffer: std.ArrayListAlignedUnmanaged(u8, alignment) = try .initCapacity(context.allocator, 1);
                defer resizeableBuffer.deinit(context.allocator);

                var failed: bool = false;
                while (true) {
                    try ctx.fileCursorMutex.lock(ctx.io);
                    errdefer ctx.fileCursorMutex.unlock(ctx.io);

                    const r = ctx.fileCursor.nextWithConfig(
                        context,
                        openConfig,
                        if (bufferType == .mmap) .mmap else .unmanaged,
                        bufferConfig,
                    );
                    // NOTE: this needs refactoring, it's shit
                    const optPath = if (ctx.fileCursor.currentPath()) |p|
                        try ctx.heapAllocator.dupe(u8, p)
                    else
                        null;
                    ctx.fileCursorMutex.unlock(ctx.io);
                    defer if (optPath) |p| ctx.heapAllocator.free(p);

                    if (r) |optStream| {
                        if (optStream == null) break;
                        var fstream = optStream.?;
                        defer {
                            fstream.close(context);
                            if (bufferType == .mmap) fstream.deinit(context);
                        }

                        switch (bufferType) {
                            .mmap => {},
                            .byte => {
                                const bufferedSize = try bufferConfig.get(fstream.stat.kind);
                                if (resizeableBuffer.capacity != bufferedSize)
                                    try resizeableBuffer.ensureTotalCapacity(context.allocator, bufferedSize);
                            },
                            .full => {
                                if (resizeableBuffer.capacity < fstream.stat.size)
                                    try resizeableBuffer.ensureTotalCapacity(context.allocator, fstream.stat.size);
                            },
                            .unmanaged => unreachable,
                        }

                        switch (bufferType) {
                            .mmap => {},
                            .byte, .full => {
                                fstream.setBuffer(alignment, resizeableBuffer.items.ptr[0..resizeableBuffer.capacity]);
                            },
                            .unmanaged => unreachable,
                        }

                        const path = optPath.?;
                        result.path = path;
                        result.fileSize = switch (fstream.stat.kind) {
                            .directory, .sym_link, .whiteout, .door, .event_port, .unknown => unreachable,
                            .block_device, .character_device, .named_pipe, .unix_domain_socket => null,
                            .file => fstream.stat.size,
                        };

                        if (fstream.stream.size) |size| {
                            fstream.fadvise(context, 0, size, &.{
                                regent.linux.FADVISE.SEQUENTIAL,
                                regent.linux.FADVISE.NOREUSE,
                            });
                        }

                        var hasher: HasherT = switch (@as(VerbEnum, @enumFromInt(verbEField.value))) {
                            .md5, .sha256 => .init(),
                            .fadler64 => .{ .flavour = verb.fadler64.positionals.tuple.@"0" },
                            .blake2b => try Blake2b.init(verb.blake2b.positionals.tuple.@"0", context.allocator),
                            inline else => .{},
                        };
                        defer if (HasherT == Blake2b) hasher.deinit();

                        var request: HashRequest(HasherT) = .{
                            .file = fstream.stream.file,
                            .reader = &fstream.stream.interface,
                            .hasher = &hasher,
                        };

                        dispatch(HasherT, ctx, &request, &result) catch |err|
                            try handleError(
                                ctx,
                                path,
                                err,
                                &failed,
                            );
                    } else |err| try handleError(
                        ctx,
                        optPath.?,
                        err,
                        &failed,
                    );
                }
                return failed;
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

pub const Blake2bCmd = struct {
    pub const HashT = Blake2b;

    pub const Positionals = positionals.PositionalOf(.{
        .TupleType = struct { Blake2b.Type },
        .ReminderType = void,
    });

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <ioFlavour> blake2b <Type> <file>"},
        .shortDescription = "Runs blake2b hashing algorithm on file",
        .description = "Runs blake2b hashing algorithm on file",
        .examples = &.{
            "flechette mmap blake2b 512 r1gb.bin",
            "flechette heap blake2b 128 r1gb.bin",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "Which blake2b flavour to run. Supported values: " ++ zcasp.help.enumValueHint(Blake2b.Type),
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
    @"recursive-follow-symlink": bool = false,
    workers: u16 = 1,
    @"worker-mode": WorkerMode = .async,

    // TODO: add file match
    // TODO: add directory match

    pub const WorkerMode = enum {
        async,
        thread,
    };

    pub const Short = .{
        .b = .benchmark,
        .ib = .@"io-benchmark",
        .ab = .@"args-benchmark",
        .u = .uppercase,
        .n = .name,
        .r = .recursive,
        .R = .@"recursive-follow-symlink",
        .w = .workers,
        .wM = .@"worker-mode",
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
        blake2b: Blake2bCmd,
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
            .{ .field = .benchmark, .description = "Prints hash benchmark on stderr." },
            .{ .field = .@"io-benchmark", .description = "Prints io benchmark on stderr. Skipped if used with --benchmark and mmap." },
            .{ .field = .@"args-benchmark", .description = "Prints args parser benchmark on stderr." },
            .{ .field = .uppercase, .description = "Prints hash in uppercase hex." },
            .{ .field = .name, .description = "Prints path." },
            .{ .field = .recursive, .description = "Will recursively follow directories. Excludes --recursive-follow-symlink." },
            .{ .field = .@"recursive-follow-symlink", .description = "Will recursively follow directories and symlinks. Excludes --recursive." },
            .{ .field = .workers, .description = "Number of workers to be used when handling --recursive or --recursive-follow-symlink. If 1, uses main thread/fiber." },
            .{
                .field = .@"worker-mode",
                .description = "Worker execution mode. Supported values: " ++ zcasp.help.enumValueHint(WorkerMode) ++ ".",
            },
        },
    };

    fn validateArgs(fbset: zcasp.validate.FieldBitSet(@This())) zcasp.validate.Error!void {
        if (fbset.allOf(.{ .recursive, .@"recursive-follow-symlink" })) return error.MutuallyExclusiveArgsPresent;
    }

    pub const GroupMatch: zcasp.validate.GroupMatchConfig(@This()) = .{
        .mandatoryVerb = true,
        .validateFn = @This().validateArgs,
    };
};

const ArgsResponse = spec.SpecResponseWithConfig(Args, zcasp.help.HelpConf{
    .backwardsBranchesQuote = 1000000,
    .simpleTypes = true,
    .headerDelimiter = "",
}, true);

const Reporter = struct {
    stdoutStream: regent.fs.FileStream(.write) = undefined,
    stderrStream: regent.fs.FileStream(.write) = undefined,
    stdoutW: *std.Io.Writer = undefined,
    stderrW: *std.Io.Writer = undefined,
    mutex: std.Io.Mutex = .init,

    pub fn init(self: *@This(), context: regent.ergo.Context) !void {
        self.stdoutStream = try regent.fs.FileStream(.write).openStream(
            context,
            std.Io.File.stdout(),
        );
        self.stdoutW = &self.stdoutStream.stream.interface;

        self.stderrStream = try regent.fs.FileStream(.write).openStream(
            context,
            std.Io.File.stderr(),
        );
        self.stderrW = &self.stderrStream.stream.interface;
        self.mutex = .init;
    }

    pub fn outIsTTY(self: *const @This()) bool {
        return self.stdoutStream.stat.kind == .character_device;
    }

    pub fn errIsTTY(self: *const @This()) bool {
        return self.stderrStream.stat.kind == .character_device;
    }

    pub fn deinit(self: *@This(), context: regent.ergo.Context) void {
        var stderrS = self.stderrStream;
        stderrS.deinit(context);
        var stdoutS = self.stdoutStream;
        stdoutS.deinit(context);
        self.stderrW.* = undefined;
        self.stdoutW.* = undefined;
        self.* = undefined;
    }
};

pub const Ctx = struct {
    stagingIo: std.Io,
    io: std.Io,
    reporter: Reporter,

    // May be fba or dba if .Debug
    stackAllocator: std.mem.Allocator,
    // May be an actual heap allocator or dba if .Debug
    heapAllocator: std.mem.Allocator,
    debugAllocator: if (builtin.mode == .Debug) *DebugAllocator else void,

    fileCursor: regent.fs.FileCursor(.read),
    fileCursorMutex: std.Io.Mutex = .init,

    pub const DebugAllocator = std.heap.DebugAllocator(.{});

    pub fn deinit(self: *@This()) void {
        if (!self.reporter.errIsTTY())
            self.reporter.stderrW.flush() catch {};
        if (!self.reporter.outIsTTY())
            self.reporter.stdoutW.flush() catch {};
        self.reporter.deinit(.{ .io = self.stagingIo, .allocator = self.stackAllocator });

        if (builtin.mode == .Debug) {
            switch (self.debugAllocator.deinit()) {
                .leak => {},
                .ok => {},
            }
        }
    }
};

pub fn main(init: std.process.Init.Minimal) !u8 {
    // NOTE: I honestly have no clue why this guy cant live in the stack
    const ctx: *Ctx = try std.heap.smp_allocator.create(Ctx);
    defer std.heap.smp_allocator.destroy(ctx);
    defer ctx.deinit();

    if (builtin.mode == .Debug) {
        var dba: Ctx.DebugAllocator = .init;
        ctx.debugAllocator = &dba;
    }

    const result = if (builtin.mode == .Debug)
        trampMain(ctx.debugAllocator.allocator(), init, ctx)
    else
        // TODO: move args parser above stack trampoline
        regent.trampoline.stackTrampoline(
            u6,
            // NOTE: this probably needs fiddling for ReleaseSafe and ReleaseSmall
            1,
            trampMain,
            .{ null, init, ctx },
        );

    return result;
}

const MainError = error{
    ErrorPartitioningStackMemory,
};

pub fn trampMain(args: struct { ?std.mem.Allocator, std.process.Init.Minimal, *Ctx }) !u8 {
    const optStackAlloc, const init, const ctx = args;
    if (optStackAlloc == null) return MainError.ErrorPartitioningStackMemory;
    ctx.stackAllocator = optStackAlloc.?;
    ctx.heapAllocator = std.heap.smp_allocator;

    ctx.stagingIo = r: {
        var io = std.Io.Threaded.init_single_threaded;
        break :r io.io();
    };

    const scrapContext: regent.ergo.Context = .{
        .allocator = ctx.stackAllocator,
        .io = ctx.stagingIo,
    };

    try ctx.reporter.init(scrapContext);

    var clock = std.Io.Clock.awake.now(scrapContext.io);
    var argsRes: ArgsResponse = .init(scrapContext.allocator);
    defer argsRes.deinit();

    if (argsRes.parseArgs(init.args)) |parseError| {
        try ctx.reporter.stderrW.print("Last opt <{?s}>, Last token <{?s}>. ", .{ parseError.lastOpt, parseError.lastToken });
        try ctx.reporter.stderrW.writeAll(parseError.message orelse unreachable);
        try ctx.reporter.stderrW.flush();
        return 1;
    }

    if (argsRes.options.@"args-benchmark") {
        try ctx.reporter.stderrW.print(
            "args parser: elapsed {d:.2}ns\n",
            .{clock.untilNow(scrapContext.io, .awake).toNanoseconds()},
        );
    }
    const paths: []const []const u8 = if (argsRes.positionals.reminder) |reminder| reminder else &.{"-"};

    const wCount = argsRes.options.workers;
    ctx.io = if (wCount == 1)
        ctx.stagingIo
    else switch (argsRes.options.@"worker-mode") {
        .async => v: {
            var evented: std.Io.Evented = undefined;
            // NOTE: using stack allocator here DESTROYS evented performance for some reason
            // TODO: Recalculate: log2_ring_entries based on workers
            try evented.init(ctx.heapAllocator, .{ .thread_limit = wCount });
            break :v evented.io();
        },
        .thread => v: {
            var tIo = std.Io.Threaded.init(ctx.heapAllocator, .{
                .stack_size = units.ByteUnit.kb * 512,
                .concurrent_limit = .limited(wCount),
            });
            break :v tIo.io();
        },
    };

    const isRecursive = argsRes.options.recursive or argsRes.options.@"recursive-follow-symlink";
    // NOTE: regent doesnt respect cancelation interally for syscalls overriden etc
    // this needs to be fixed
    ctx.fileCursor = regent.fs.FileCursor(.read).initWithConfig(paths, .{
        .recursive = isRecursive,
        .followSymlink = argsRes.options.@"recursive-follow-symlink",
    });
    ctx.fileCursorMutex = .init;

    // this is as best effort as it gets
    if (builtin.mode == .Debug) regent.ergo.assertDeepNotUndefined(ctx.*);

    const ioFlavour: IOFlavour = argsRes.positionals.tuple.@"0";

    // TODO: comptime skip lock acquire logic for better single threaded perf
    if (wCount == 1 or !isRecursive) {
        const hadErrors = try ioFlavour.run(&argsRes, ctx);
        return if (hadErrors) 1 else 0;
    } else {
        const FutureT = std.Io.Future(@typeInfo(@TypeOf(IOFlavour.run)).@"fn".return_type.?);
        const futures: []FutureT = try scrapContext.allocator.alloc(FutureT, wCount);
        defer scrapContext.allocator.free(futures);

        for (futures) |*future|
            future.* = switch (argsRes.options.@"worker-mode") {
                .async => ctx.io.async(IOFlavour.run, .{ &ioFlavour, &argsRes, ctx }),
                .thread => try ctx.io.concurrent(IOFlavour.run, .{ &ioFlavour, &argsRes, ctx }),
            };

        var hadErrors: bool = false;
        var i: usize = 0;
        while (i < futures.len) : (i += 1) {
            hadErrors |= futures[i].await(ctx.io) catch |e| {
                while (i < futures.len) : (i += 1) {
                    _ = futures[i].cancel(ctx.io) catch {};
                }
                return e;
            };
        }
        return if (hadErrors) 1 else 0;
    }
}

const std = @import("std");
const zpec = @import("zpec");
const args = zpec.args;
const coll = zpec.collections;
const spec = args.spec;
const codec = zpec.args.codec;
const positionals = args.positionals;
const HelpData = args.help.HelpData;
const PositionalOf = positionals.PositionalOf;
const SpecResponse = spec.SpecResponse;
const Cursor = coll.Cursor;
const AsCursor = zpec.collections.AsCursor;
const adler = @import("adler.zig");
const fadler = @import("fadler.zig");

fn printReport(
    hashType: []const u8,
    chunkIndex: usize,
    writer: *std.Io.Writer,
    hash: anytype,
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
            hashType,
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

pub fn ioWithMmap(T: type, hasher: *T, verb: *const Res.VerbT, w: *std.Io.Writer, path: []const u8) !void {
    var timer = try std.time.Timer.start();

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
    defer std.posix.munmap(ptr);

    hasher.roll(ptr);

    try printReport(
        @tagName(verb.*),
        1,
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

pub fn ioWithBuffer(T: type, hasher: *T, verb: *const Res.VerbT, w: *std.Io.Writer, path: []const u8) !void {
    var timer = try std.time.Timer.start();

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
        hasher.roll(buff[0..chunkLen]);

        chunkIndex += 1;
        bytesProcessed += chunkLen;
    }

    try printReport(
        @tagName(verb.*),
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
        verb: *const Res.VerbT,
        w: *std.Io.Writer,
        path: []const u8,
    ) !void {
        inline for (std.meta.fields(Res.VerbT)) |field| {
            if (std.mem.eql(u8, field.name, @tagName(verb.*))) {
                const Hasher: type = field.type.Options.hasher();
                var hasher: Hasher = if (Hasher == fadler) .{
                    .flavour = switch (verb.*) {
                        .fadler => |cmd| cmd.positionals.tuple.@"0",
                        else => unreachable,
                    },
                } else .{};
                return switch (self.*) {
                    .mmap => try ioWithMmap(Hasher, &hasher, verb, w, path),
                    .buffered => try ioWithBuffer(Hasher, &hasher, verb, w, path),
                };
            }
        } else {
            @panic("Unrecognizeable Verb passed to IOFlavour");
        }
    }
};

pub const PathCodec = struct {
    pathBuff: [4098]u8 = undefined,

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
            const path = cursor.next() orelse return Error.MissingPath;
            return try std.fs.cwd().realpath(path, &self.pathBuff);
        } else {
            return codec.PrimitiveCodec.parseByType(self, T, tag, allc, cursor);
        }
    }
};

// TODO: move to args parser help
pub fn enumValueHint(target: type) []const u8 {
    return comptime rv: {
        var b = coll.ComptSb.init("{ ");
        const fields = @typeInfo(target).@"enum".fields;
        for (fields, 0..) |field, i| {
            b.append(field.name);
            if (i + 1 < fields.len) b.append(", ");
        }
        b.append(" }");
        break :rv b.s;
    };
}

pub fn AdlerCmd(adlerType: adler.AdlerType) type {
    return struct {
        pub fn hasher() type {
            return adler.AdlerHash(adlerType);
        }

        pub const Positionals = PositionalOf(.{
            .TupleType = void,
            .ReminderType = void,
        });

        pub const Help: HelpData(@This()) = .{
            .usage = &.{"flechette <iotype> " ++ @tagName(adlerType) ++ " <file>"},
            .shortDescription = "Runs " ++ @tagName(adlerType) ++ " hashing algorithm on file",
            .description = "Runs " ++ @tagName(adlerType) ++ " hashing algorithm on file",
            .examples = &.{
                "flechette mmap " ++ @tagName(adlerType) ++ " random_250mb.bin",
                "flechette buffered " ++ @tagName(adlerType) ++ " random_1gb.bin",
            },
        };

        pub const HelpFmt = args.help.HelpFmt(@This(), .{ .simpleTypes = true, .optionsBreakline = true });
    };
}

pub const FadlerCmd = struct {
    pub fn hasher() type {
        return fadler;
    }

    pub const Positionals = PositionalOf(.{
        .TupleType = struct { fadler.FadlerFlavour },
        .ReminderType = void,
    });

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <iotype> fadler <fladerFlavour> <file>"},
        .shortDescription = "Runs flader hashing algorithm on file",
        .description = "Runs flader hashing algorithm on file",
        .examples = &.{
            "flechette mmap fadler hdiff random_1gb.bin",
            "flechette buffered fadler scalar16 random_1gb.bin",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "Which fadler flavour to run. Supported values: " ++ enumValueHint(fadler.FadlerFlavour),
            },
        },
    };

    pub const HelpFmt = args.help.HelpFmt(@This(), .{ .simpleTypes = true, .optionsBreakline = true });
};

pub const Args = struct {
    pub const Positionals = PositionalOf(.{
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
        fadler: FadlerCmd,
    };

    pub const Help: HelpData(@This()) = .{
        .usage = &.{"flechette <iotype> <command> <file>"},
        .description = "Cli to run hashing algorithms on a file treated as binary",
        .examples = &.{
            "flechette mmap adler64 random_1gb.bin",
            "flechette buffered adler32 random_1gb.bin",
            "flechette mmap fadler hdiff random_1gb.bin",
        },
        .positionalsDescription = .{
            .tuple = &.{
                "IOFlavour to use to read the binary. Supported values: " ++ enumValueHint(IOFlavour),
                "file path (relative)",
            },
        },
    };

    pub const GroupMatch: args.validate.GroupMatchConfig(@This()) = .{
        .mandatoryVerb = true,
    };

    pub const HelpFmt = args.help.HelpFmt(@This(), .{ .simpleTypes = true, .optionsBreakline = true });
};

const Res = SpecResponse(Args);

pub fn main() !u8 {
    var sfba = std.heap.stackFallback(4098, std.heap.page_allocator);
    const allocator = sfba.get();

    var buff: [1024]u8 = undefined;
    var stderrW = std.fs.File.stderr().writer(&buff);
    var w = &stderrW.interface;

    var res: Res = .init(allocator);
    if (res.parseArgs()) |parseError| {
        try w.writeAll(parseError.message orelse "");
        try w.flush();
        return 1;
    }

    const ioFlavour: IOFlavour, const path = res.positionals.tuple;
    try ioFlavour.run(&res.verb.?, w, path);

    return 0;
}

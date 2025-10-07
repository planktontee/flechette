hash: u64 = 1,
firstRun: bool = true,
flavour: FadlerFlavour,

const fadler64Table: *const [256]u32 = &.{
    0xb5e6a199, 0xe9834671, 0x8c715a7f, 0x0914fc22, 0x62abb5af, 0x9d31fb39, 0x69f00344, 0xa3d3c035,
    0x64db3b1c, 0x399a5b72, 0x959bb01e, 0x0c036c4f, 0x344fbc39, 0x4dc75b77, 0x25be7198, 0x0ab026b3,
    0xbab2d5e6, 0xe6a1281e, 0x0eb13df7, 0x53346d23, 0x43437647, 0xb363ede9, 0x5783a9eb, 0xdea89be5,
    0x4a2e1c06, 0x7b58e553, 0xd3979639, 0xc1fafc95, 0xb5ec388c, 0xb0db4075, 0xc573fb3d, 0x4cde0617,
    0x4b86ee95, 0xd77edb52, 0x100140c5, 0x6527c8e8, 0x8b8e01cb, 0x3bb6faa9, 0x9f77583e, 0xd11925ac,
    0x8ae986f0, 0x59380e03, 0x3a9b560b, 0xad9eb601, 0x0f16648e, 0x36b3f894, 0xfe25d3b7, 0x14a50768,
    0xaaed25fd, 0x06cd0894, 0x667abac1, 0x5833a3c9, 0x92bfd5cf, 0xd9674e5e, 0x00b7f263, 0xa36fa10b,
    0x12338782, 0xcb11edb6, 0xca15bc13, 0xd10e177f, 0x4c60b16c, 0x762153c1, 0x059207f3, 0x6f8d3213,
    0xef298d69, 0xdba6f8b4, 0x15056856, 0x409f8150, 0x3b7609d6, 0xf15a0a04, 0x53b2fcc9, 0x8ad571be,
    0xa40dcc34, 0xe127ab04, 0xdd1a7d81, 0xfe33ae6b, 0x10e26f52, 0x45791ae1, 0x9208538c, 0xc5281840,
    0x444fee94, 0xa7b050a1, 0x0e053633, 0xaa159ddd, 0xc3c99a7b, 0x70e88e7a, 0x40f5c699, 0x5627f1ae,
    0x7c216685, 0x90550ccc, 0x5b565b88, 0x22ca3af3, 0x3f5c8372, 0x856d11ad, 0xd749378c, 0x70e5fc79,
    0x0ad88005, 0x5fbea93e, 0x9119cf5e, 0xb2108f9f, 0xd069b0e6, 0xd01b096b, 0x3bbbef86, 0x9d2e92ef,
    0x8b0b3b0b, 0xd853a1a8, 0x7773f1f2, 0x31450766, 0x96cefd47, 0x935a1c07, 0xb6f5245a, 0x4d75b877,
    0xc4e0ae38, 0xf291709d, 0x5d4a7395, 0xeaf6717b, 0x353013a2, 0x14ab729d, 0x20560099, 0x8d2f2776,
    0x7ce6ccb9, 0x4832d5cd, 0xd4519fa3, 0x0cac0dd3, 0x008ba27d, 0x0360ed98, 0x6fb2f7c0, 0x12f6b6ad,
    0x694823dd, 0x82441585, 0x2317f193, 0x09aa5ce1, 0x92dffce9, 0x4a06eddb, 0x80095313, 0x95613181,
    0x3a4b5d4b, 0x2dd5ab17, 0xb4ad20be, 0xd4297df4, 0x881cb3c5, 0x61036b5e, 0x0d271952, 0x5ca6be40,
    0x1c13ed9a, 0xd1a39288, 0x85d5c7ff, 0x76bd0c57, 0x900595da, 0x8ee9e2bc, 0xadeaeb12, 0x77b940bf,
    0x6589001c, 0x1e1a09de, 0x4c31dc7f, 0x3bd372be, 0x86af13eb, 0x37279e1b, 0x9329fcc0, 0xb9e4d0d0,
    0xd0d7f7b6, 0x00800dd5, 0xa9cd017f, 0xd9b49a97, 0xc6b9318c, 0x107a9974, 0x0e3d65c5, 0x1e030a54,
    0x71a2ae9a, 0x298db8ca, 0xdd7e2ea6, 0x7c7e43c4, 0x41d5b0d5, 0x5f35be61, 0xbc4a4198, 0x33b43e0c,
    0xa0e21f24, 0xd2cfb061, 0x60963539, 0x8c917984, 0xc86fc710, 0x08f07c80, 0x044d34fb, 0x1287cd98,
    0xbebea104, 0xa0aeab0b, 0xb775e218, 0xd75dc20e, 0xc79211cb, 0x88cab942, 0x19caa6e3, 0x69a032d1,
    0x3d9db18b, 0x3658c36c, 0x8deb277c, 0xaa2bd1e8, 0xa6ea28e4, 0xebd02f0c, 0x9de0ccd9, 0x0d6e8665,
    0xca028b95, 0x68189e89, 0x5b18e944, 0xf4c654ee, 0x03824598, 0x26d7a9ea, 0xbbb5de59, 0xc6b26613,
    0x88152a10, 0x5abd6d33, 0x8fb51b5c, 0x33a9fe83, 0x8b82720e, 0x4cc28094, 0x6687f674, 0x5442aac8,
    0x1369f26f, 0x99f94f7d, 0x49dd656e, 0xe3b5bb25, 0xe9d7524a, 0xceab6332, 0xa8d417d5, 0x5e730186,
    0x3ff82ff0, 0x38cda2e2, 0x9da9e9f2, 0xb1eb6f6f, 0xe2643584, 0xc3fe12ad, 0x6a6621ce, 0x18054b2e,
    0x880c75bb, 0x55c725b1, 0xd6e226a9, 0x04747a86, 0x3ba442af, 0xde31e4b6, 0x56b265b8, 0x90be6f06,
    0xd491b721, 0xe6004b58, 0x7e737445, 0xb1353152, 0x689bee09, 0xd9ae2437, 0x1e4160bc, 0x761828fc,
    0x9e290dc5, 0xdae6bc72, 0xb64afb06, 0x5198300d, 0x42d6d3f1, 0x08007323, 0x8cf682e4, 0xa1606c21,
};

fn scalar16(data: []const u8, a: u64, b: u64) u64 {
    var adler = a;
    var sum = b;
    var len = data.len;
    var ptr = data.ptr;

    while (len >= 16) {
        const a0 = fadler64Table[ptr[0]];
        const a1 = fadler64Table[ptr[1]];
        const a2 = fadler64Table[ptr[2]];
        const a3 = fadler64Table[ptr[3]];
        const a4 = fadler64Table[ptr[4]];
        const a5 = fadler64Table[ptr[5]];
        const a6 = fadler64Table[ptr[6]];
        const a7 = fadler64Table[ptr[7]];
        const a8 = fadler64Table[ptr[8]];
        const a9 = fadler64Table[ptr[9]];
        const a10 = fadler64Table[ptr[10]];
        const a11 = fadler64Table[ptr[11]];
        const a12 = fadler64Table[ptr[12]];
        const a13 = fadler64Table[ptr[13]];
        const a14 = fadler64Table[ptr[14]];
        const a15 = fadler64Table[ptr[15]];

        adler += a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 + a14 + a15;
        sum += adler * 16 - a0 * 0 - a1 * 1 - a2 * 2 - a3 * 3 - a4 * 4 - a5 * 5 - a6 * 6 - a7 * 7 - a8 * 8 - a9 * 9 - a10 * 10 - a11 * 11 - a12 * 12 - a13 * 13 - a14 * 14 - a15 * 15;
        len -= 16;
        ptr += 16;
    }

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | (@as(u64, @intCast(adler)) & 0xFFFFFFFF);
}

fn scalar8(data: []const u8, a: u64, b: u64) u64 {
    var adler = a;
    var sum = b;
    var len = data.len;
    var ptr = data.ptr;

    while (len >= 8) {
        const a0 = fadler64Table[ptr[0]];
        const a1 = fadler64Table[ptr[1]];
        const a2 = fadler64Table[ptr[2]];
        const a3 = fadler64Table[ptr[3]];
        const a4 = fadler64Table[ptr[4]];
        const a5 = fadler64Table[ptr[5]];
        const a6 = fadler64Table[ptr[6]];
        const a7 = fadler64Table[ptr[7]];

        adler += a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7;
        sum += adler * 8 - a0 * 0 - a1 * 1 - a2 * 2 - a3 * 3 - a4 * 4 - a5 * 5 - a6 * 6 - a7 * 7;
        len -= 8;
        ptr += 8;
    }

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | (@as(u64, @intCast(adler)) & 0xFFFFFFFF);
}

fn scalar4(data: []const u8, a: u64, b: u64) u64 {
    var adler = a;
    var sum = b;
    var len = data.len;
    var ptr = data.ptr;

    while (len >= 4) {
        const a0 = fadler64Table[ptr[0]];
        const a1 = fadler64Table[ptr[1]];
        const a2 = fadler64Table[ptr[2]];
        const a3 = fadler64Table[ptr[3]];

        adler += a0 + a1 + a2 + a3;
        sum += adler * 4 - a0 * 0 - a1 * 1 - a2 * 2 - a3 * 3;
        len -= 4;
        ptr += 4;
    }

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | (@as(u64, @intCast(adler)) & 0xFFFFFFFF);
}

fn scalar2(data: []const u8, a: u64, b: u64) u64 {
    var adler = a;
    var sum = b;
    var len = data.len;
    var ptr = data.ptr;

    while (len >= 2) {
        const a0 = fadler64Table[ptr[0]];
        const a1 = fadler64Table[ptr[1]];

        adler += a0 + a1;
        sum += adler * 2 - a0 * 0 - a1 * 1;

        len -= 2;
        ptr += 2;
    }

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | (@as(u64, @intCast(adler)) & 0xFFFFFFFF);
}

fn hdiff(data: []const u8, a: u64, b: u64) u64 {
    var adler: u32 = @intCast(a);
    var sum: u32 = @intCast(b);
    var len = data.len;
    var ptr = data.ptr;

    while (len >= 4) {
        const idx0, const idx1, const idx2, const idx3 = ptr[0..4].*;

        const a1 = fadler64Table[idx1];
        var a2 = fadler64Table[idx2];
        const a3 = fadler64Table[idx3];
        const a0 = fadler64Table[idx0] + adler;

        adler = a0 + a1;
        sum += a0 * 2 + a1;

        a2 += adler;
        adler = a2 + a3;
        sum += a2 * 2 + a3;

        len -= 4;
        ptr += 4;
    }

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | @as(u64, @intCast(adler));
}

fn scalar(data: []const u8, a: u64, b: u64) u64 {
    var adler: u32 = @intCast(a);
    var sum: u32 = @intCast(b);
    var len = data.len;
    var ptr = data.ptr;

    while (len > 0) : (len -= 1) {
        adler += fadler64Table[ptr[0]];
        sum += adler;
        ptr += 1;
    }

    return (@as(u64, @intCast(sum)) << 32) | @as(u64, @intCast(adler));
}

// NOTE: this is technically combining, not rolling
pub fn roll(self: *@This(), data: []const u8) void {
    const newHash = switch (self.flavour) {
        .scalar => scalar(data, 1, 0),
        .hdiff => hdiff(data, 1, 0),
        .scalar2 => scalar2(data, 1, 0),
        .scalar4 => scalar4(data, 1, 0),
        .scalar8 => scalar8(data, 1, 0),
        .scalar16 => scalar16(data, 1, 0),
    };

    if (self.firstRun) {
        self.firstRun = false;
        self.hash = newHash;
    } else {
        @branchHint(.likely);
        // Combine defined here: https://github.com/sisong/HDiffPatch/blob/master/libHDiffPatch/HDiff/private_diff/limit_mem_diff/adler_roll.c#L357
        const rAdler = newHash;
        const rSum = newHash >> 32;
        const lAdler = self.hash;
        const lSum = self.hash >> 32;

        const rlen = data.len;
        var sum = rlen * lAdler;
        const adler = lAdler + rAdler - 1;
        sum += lSum + rSum - rlen;

        self.hash = (sum << 32) | (adler & 0xFFFFFFFF);
    }
}

pub const FadlerFlavour = enum {
    hdiff,
    scalar,
    scalar2,
    scalar4,
    scalar8,
    scalar16,
};

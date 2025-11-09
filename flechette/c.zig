pub const c = @cImport({
    @cDefine("_GNU_SOURCE", "");
    @cDefine("_FILE_OFFSET_BITS", "64");
    @cInclude("fcntl.h");
    @cInclude("stdlib.h");
    @cInclude("openssl/md5.h");
});

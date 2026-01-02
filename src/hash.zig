const std = @import("std");

pub const Murmur3 = struct {
    const C1: u64 = 0x87c37b91114253d5;
    const C2: u64 = 0x4cf5ad432745937f;

    pub fn hash(key: []const u8) u64 {
        var h1: u64 = 0;
        var h2: u64 = 0;
        const len = key.len;

        var i: usize = 0;
        while (i + 16 <= len) : (i += 16) {
            const k1 = std.mem.readInt(u64, key[i..][0..8], .little);
            const k2 = std.mem.readInt(u64, key[i + 8 ..][0..8], .little);

            h1 ^= mixK1(k1);
            h1 = std.math.rotl(u64, h1, 27);
            h1 = h1 +% h2;
            h1 = h1 *% 5 +% 0x52dce729;

            h2 ^= mixK2(k2);
            h2 = std.math.rotl(u64, h2, 31);
            h2 = h2 +% h1;
            h2 = h2 *% 5 +% 0x38495ab5;
        }

        var k1: u64 = 0;
        var k2: u64 = 0;
        const remaining = len - i;
        if (remaining > 0) {
            const tail = key[i..];
            switch (remaining) {
                15 => {
                    k2 ^= @as(u64, tail[14]) << 48;
                    k2 ^= @as(u64, tail[13]) << 40;
                    k2 ^= @as(u64, tail[12]) << 32;
                    k2 ^= @as(u64, tail[11]) << 24;
                    k2 ^= @as(u64, tail[10]) << 16;
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                14 => {
                    k2 ^= @as(u64, tail[13]) << 40;
                    k2 ^= @as(u64, tail[12]) << 32;
                    k2 ^= @as(u64, tail[11]) << 24;
                    k2 ^= @as(u64, tail[10]) << 16;
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                13 => {
                    k2 ^= @as(u64, tail[12]) << 32;
                    k2 ^= @as(u64, tail[11]) << 24;
                    k2 ^= @as(u64, tail[10]) << 16;
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                12 => {
                    k2 ^= @as(u64, tail[11]) << 24;
                    k2 ^= @as(u64, tail[10]) << 16;
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                11 => {
                    k2 ^= @as(u64, tail[10]) << 16;
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                10 => {
                    k2 ^= @as(u64, tail[9]) << 8;
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                9 => {
                    k2 ^= @as(u64, tail[8]);
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                8 => {
                    k1 ^= std.mem.readInt(u64, tail[0..8], .little);
                },
                7 => {
                    k1 ^= @as(u64, tail[6]) << 48;
                    k1 ^= @as(u64, tail[5]) << 40;
                    k1 ^= @as(u64, tail[4]) << 32;
                    k1 ^= @as(u64, tail[3]) << 24;
                    k1 ^= @as(u64, tail[2]) << 16;
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                6 => {
                    k1 ^= @as(u64, tail[5]) << 40;
                    k1 ^= @as(u64, tail[4]) << 32;
                    k1 ^= @as(u64, tail[3]) << 24;
                    k1 ^= @as(u64, tail[2]) << 16;
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                5 => {
                    k1 ^= @as(u64, tail[4]) << 32;
                    k1 ^= @as(u64, tail[3]) << 24;
                    k1 ^= @as(u64, tail[2]) << 16;
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                4 => {
                    k1 ^= @as(u64, tail[3]) << 24;
                    k1 ^= @as(u64, tail[2]) << 16;
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                3 => {
                    k1 ^= @as(u64, tail[2]) << 16;
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                2 => {
                    k1 ^= @as(u64, tail[1]) << 8;
                    k1 ^= @as(u64, tail[0]);
                },
                1 => {
                    k1 ^= @as(u64, tail[0]);
                },
                else => unreachable,
            }
            h1 ^= mixK1(k1);
            h2 ^= mixK2(k2);
        }

        h1 ^= len;
        h2 ^= len;

        h1 = h1 +% h2;
        h2 = h2 +% h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 = h1 +% h2;

        return h1;
    }

    fn mixK1(k: u64) u64 {
        var k1 = k;
        k1 = k1 *% C1;
        k1 = std.math.rotl(u64, k1, 31);
        k1 = k1 *% C2;
        return k1;
    }

    fn mixK2(k: u64) u64 {
        var k2 = k;
        k2 = k2 *% C2;
        k2 = std.math.rotl(u64, k2, 33);
        k2 = k2 *% C1;
        return k2;
    }

    fn fmix64(k: u64) u64 {
        var x = k;
        x ^= x >> 33;
        x = x *% 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x = x *% 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return x;
    }
};

pub const Crc32 = struct {
    pub fn hash(key: []const u8) u64 {
        const h = std.hash.Crc32.hash(key);
        var res: u64 = h;
        // Java's Crc32Hash implementation:
        // long h = crc.getValue();
        // h |= h << 32;
        // return h;
        res = res | (res << 32);
        return res;
    }
};

test "Murmur3 sanity check" {
    const key = "hello";
    const h = Murmur3.hash(key);
    try std.testing.expectEqual(@as(u64, 0xcbd8a7b341bd9b02), h);
}

test "Crc32 sanity check" {
    const key = "hello";
    // Java CRC32("hello") = 907060870
    // Result = 907060870 | (907060870 << 32)
    // 0x3610a686 | 0x3610a68600000000 = 0x3610a6863610a686

    const h = Crc32.hash(key);
    try std.testing.expectEqual(@as(u64, 0x3610a6863610a686), h);
}

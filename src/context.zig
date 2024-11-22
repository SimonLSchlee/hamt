//! A context is used to compare keys and iterate over chunk
//! sized bitfragments derived from that key.
//!
//! The `HashContext` hashes the key and then iterates over bits of that hash.
//!
//! The `PrefixContext` iterates over bits of that key directly
//! and only should be used when the sorted order of keys is needed,
//! because using it could lead to deep and unbalanced tries,
//! increasing insertion and lookup time.
const std = @import("std");

/// Function used to compare keys for equality.
pub const getAutoEqlFn = std.hash_map.getAutoEqlFn;

/// Get the size of a fixed size key in bits,
/// if the type isn't supported to be used as a fixed size key,
/// the function returns null.
pub fn getFixedSizeKeyBits(comptime Key: type) ?comptime_int {
    return switch (@typeInfo(Key)) {
        .Int => |i| i.bits,
        .Enum => |e| std.math.log2_int_ceil(usize, e.fields.len),
        else => null,
    };
}

/// Gets the bits of a fixed size key, or creates a compile error if the key isn't supported.
pub fn fixedSizeKeyBits(comptime Key: type) comptime_int {
    return if (getFixedSizeKeyBits(Key)) |bits| bits else @compileError("fixedSizeKeyBits not implemented/supported for: " ++ @typeName(Key));
}

/// An iterator type that iterates over a `Key` in `Chunk` sized portions of bits.
pub fn BitChunkIterator(comptime Chunk: type, comptime Key: type) type {
    const key_bits = fixedSizeKeyBits(Key);
    const chunk_bits = fixedSizeKeyBits(Chunk);
    const chunk_count = @divTrunc(key_bits, chunk_bits);

    // For example if we have a 64 bit key and a 5 bit chunk size
    // we end up with the last chunk:
    // 60 61 62 63 00  <- next key-hash
    //  0  1  2  3  4
    //              ^--- this bit is never used
    //                   which results in a tree with less branches
    //                   thus more depth
    //
    // this is only a problem if the mismatch is very strong
    // maybe we can even calculate some heuristic when we care and when not?
    //
    // for example for non-hashed keys we probably never care
    // for slight mismatches with low probability of occuring we probably also don't care
    // }

    return struct {
        const Self = @This();
        data: Key,
        i: u6 = 0,

        const chunk_mask: Chunk = ~@as(Chunk, 0);

        pub fn next(self: *Self) ?Chunk {
            return if (self.i == chunk_count) null else res: {
                const c = self.i;
                self.i +%= 1; // avoiding redundant bounds checks
                const chunk = self.data >> (chunk_bits * c);
                const res: Chunk = @intCast(chunk & chunk_mask);
                break :res res;
            };
        }
    };
}

/// Instantiates a `BitChunkIterator`.
pub fn bitChunkIterator(comptime Chunk: type, key: anytype) BitChunkIterator(Chunk, @TypeOf(key)) {
    return .{ .data = key };
}

/// An iterator that returns an infinite sequence of chunks, rehashing with an increased depth
/// whenever it reaches the end of the current hash.
pub fn ChunkedRehashIterator(comptime Chunk: type, comptime Hasher: type) type {
    return struct {
        const Self = @This();
        hasher: Hasher,
        depth: u16 = 0,
        iter: BitChunkIterator(Chunk, u64) = undefined,

        pub fn init(hasher: Hasher) @This() {
            return .{ .hasher = hasher };
        }

        pub fn setup(self: *Self) void {
            self.iter = bitChunkIterator(Chunk, self.hasher.final());
        }

        pub fn next(self: *Self) ?Chunk {
            if (self.iter.next()) |chunk| {
                return chunk;
            } else {
                self.depth += 1;
                std.hash.autoHash(&self.hasher, self.depth);

                const hash = self.hasher.final();
                self.iter = bitChunkIterator(Chunk, hash);
                return self.next();
            }
        }
    };
}

/// Instantiates a `ChunkedRehashIterator`.
pub fn chunkedRehashIterator(comptime Chunk: type, hasher: anytype) ChunkedRehashIterator(Chunk, @TypeOf(hasher)) {
    return ChunkedRehashIterator(Chunk, @TypeOf(hasher)).init(hasher);
}

/// This context calculates a hash from the key type and then iterates over Chunk
/// sized fragments of that hash.
pub fn HashContext(comptime K: type) type {
    return struct {
        const Self = @This();

        pub const eql = getAutoEqlFn(K, @This());

        pub fn prefix(self: Self, comptime Chunk: type, key: K) ChunkedRehashIterator(Chunk, std.hash.Wyhash) {
            _ = self;

            // TODO maybe the current strategy does not work? because if 2 keys result in the same hash (collide)
            // then just adding depth values to the keys won't make them distinct from another
            // so maybe instead of zero for init we should use the depth as the value for init
            // this way we start with a different seed value and then using that different starting point
            // we call hash_fn again so it can generate different entropy based on the different seed value
            // TODO create a test code base that generates/finds colliding hashes plots them, randomized tests, etc.
            var hasher = std.hash.Wyhash.init(0);
            std.hash.autoHashStrat(&hasher, key, .Deep);
            var iter = chunkedRehashIterator(Chunk, hasher);
            iter.setup();
            return iter;
        }
    };
}

fn AnyBitIterator(comptime Chunk: type) type {
    return struct {
        const Self = @This();
        const chunk_bits = fixedSizeKeyBits(Chunk);

        pub const BitReader = std.io.BitReader(.little, std.io.AnyReader);
        bit_reader: BitReader,

        pub fn init(byte_reader: std.io.AnyReader) Self {
            return .{ .bit_reader = BitReader.init(byte_reader) };
        }

        pub fn next(self: *Self) ?Chunk {
            var read_bits: usize = 0;
            const chunk = self.bit_reader.readBits(Chunk, chunk_bits, &read_bits) catch |err| std.debug.panic("unexpected error: {any}", .{err});
            if (read_bits == 0) return null;
            return chunk;
        }
    };
}

fn asBytes(ptr: anytype) []const u8 {
    const info = @typeInfo(@TypeOf(ptr));
    switch (info) {
        .Pointer => |p| {
            if (p.child == []const u8) return ptr.*;
            const info2 = @typeInfo(p.child);
            switch (info2) {
                .Pointer => |p2| {
                    if (p2.size == .Slice) {
                        const bytes = @sizeOf(p2.child) * ptr.len;
                        const mem: [*]const u8 = @ptrCast(ptr.ptr);
                        return mem[0..bytes];
                    }
                },
                else => {},
            }

            return std.mem.asBytes(ptr);
        },
        else => @compileError("not implemented: " ++ @tagName(info)),
    }
}

/// A generic iterator that uses a `GenericReader` to iterate over bytes and then returns chunks of bits.
fn ByteBitIterator(comptime Chunk: type, comptime Key: type) type {
    return struct {
        const Self = @This();

        const Stream = std.io.FixedBufferStream([]const u8);
        const GenericReader = std.io.Reader(*Stream, error{}, Stream.read);
        key: Key,
        stream: ?Stream = null,
        reader: ?GenericReader = null,
        it: ?AnyBitIterator(Chunk) = null,

        /// Used to create the iterator, call the `setup` function before using the iterator.
        pub fn init(key: Key) Self {
            return .{ .key = key };
        }
        /// Used to finish initialisation and setup internal fields.
        pub fn setup(self: *Self) void {
            const bytes: []const u8 = asBytes(&self.key);
            self.stream = std.io.fixedBufferStream(bytes);
            self.reader = self.stream.?.reader();
            self.it = AnyBitIterator(Chunk).init(self.reader.?.any());
        }

        pub fn next(self: *Self) ?Chunk {
            return if (self.it) |*i| i.next() else null;
        }
    };
}

/// This context doesn't hash the key and instead uses bits from the key directly,
/// only do this if you are sure the key has a good distribution,
/// otherwise you may end up with very unbalanced hash array mapped tries,
/// that can lead to increased memory usage and lookup times.
pub fn PrefixContext(comptime K: type) type {
    return struct {
        const Self = @This();

        pub const eql = getAutoEqlFn(K, @This());

        pub fn prefix(_: Self, comptime Chunk: type, key: K) ByteBitIterator(Chunk, @TypeOf(key)) {
            return ByteBitIterator(Chunk, @TypeOf(key)).init(key);
        }
    };
}

/// Calls the setup function of iterators that have this function, to finish initialisation.
pub inline fn iterSetup(iter: anytype) void {
    const Iter = std.meta.Child(@TypeOf(iter));
    if (@hasDecl(Iter, "setup")) iter.setup();
}

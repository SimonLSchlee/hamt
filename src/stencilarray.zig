const std = @import("std");
const custom = @import("customallocator.zig");

// Runtime and Compiler Support for HAMTs
// https://www-old.cs.utah.edu/plt/publications/dls21-tzf.pdf

pub fn HeadNode(comptime MaskInt: type, comptime MetaInt: type) type {
    return packed struct {
        const Self = @This();
        mask: MaskInt,
        meta: MetaInt,

        pub const zero = Self{ .mask = 0, .meta = 0 };

        pub fn size(self: Self) usize {
            return @popCount(self.mask) +% 1;
        }
        pub fn count(self: Self) usize {
            return @popCount(self.mask);
        }
        pub fn empty(self: Self) bool {
            return self.count() == 0;
        }
    };
}

pub fn StencilArray(comptime max_size: u16, comptime T: type, comptime Allocator: type) type {
    comptime custom.checkAllocator(Allocator);
    const is_indexed = custom.isRefAllocator(Allocator);
    return struct {
        const Self = @This();
        fn other(comptime allocator: type) type {
            return StencilArray(max_size, T, allocator);
        }
        fn otherSelf(comptime allocator: type) type {
            return other(allocator);
        }

        const calculations = CheckAndCalculate(max_size, T);
        pub const MaskInt = calculations.MaskInt;
        pub const MaskShift = calculations.MaskShift;
        pub const MetaBits = calculations.MetaBits;
        pub const MetaInt = calculations.MetaInt;
        const node_size = calculations.node_size;

        pub const Head = HeadNode(MaskInt, MetaInt);
        pub const alignment = @max(@alignOf(Head), @alignOf(T));

        /// Calculates where the memory for `Extra` begins, it is located after the normal nodes.
        pub fn extraStart(nodes: usize, comptime Extra: type) usize {
            const extra_align = comptime @alignOf(Extra);
            const normal_nodes_size = node_size * nodes;
            const extra_start = std.mem.alignForward(usize, normal_nodes_size, extra_align);
            return extra_start;
        }
        fn divCeil(numerator: usize, denominator: usize) usize {
            return @divFloor(numerator - 1, denominator) + 1;
        }
        /// Calculates the total number of nodes required to store all the data including `Extra`.
        pub fn totalNodes(nodes: usize, comptime Extra: type) usize {
            const extra_start = extraStart(nodes, Extra);
            const end = extra_start + @sizeOf(Extra);
            const total_nodes = divCeil(end, node_size);
            return total_nodes;
        }
        /// Allocates the needed memory, returning a `Ref` to it.
        pub fn allocExtra(allocator: Allocator, nodes: usize, comptime Extra: type) !Ref {
            const total_nodes = totalNodes(nodes, Extra);
            if (std.meta.hasFn(Allocator, "alignedAlloc")) {
                const ref_or_slice = try allocator.alignedAlloc(T, alignment, @intCast(total_nodes));
                return if (is_indexed) ref_or_slice else @ptrCast(@alignCast(ref_or_slice.ptr));
            } else {
                const ref_or_slice = try allocator.alloc(T, @intCast(total_nodes));
                return if (is_indexed) ref_or_slice else @ptrCast(@alignCast(ref_or_slice.ptr));
            }
        }

        const Ref = if (is_indexed) custom.LookupFunctionGetReference(Allocator) else *Head;
        ref: Ref,

        const RefLookup = struct {
            pub const deref = RefLookup.lookup;
            pub inline fn lookup(self: Self, allocator: Allocator) SA {
                return .{ .head = @ptrCast(@alignCast(allocator.lookup(self.ref).ptr)) };
            }
        };
        const PtrLookup = struct {
            pub inline fn deref(self: Self, allocator: Allocator) SA {
                _ = allocator;
                return .{ .head = self.ref };
            }
            pub inline fn lookup(self: Self) SA {
                return .{ .head = self.ref };
            }
        };
        /// Lookup converts ref to the value it refers to. If you want to write code that is Allocator-agnostic use `deref`.
        pub const lookup = if (is_indexed) RefLookup.lookup else PtrLookup.lookup;
        /// Deref is the Allocator-agnostic variant of `lookup` converting either a pointer or index to the handle it refers to.
        pub const deref = if (is_indexed) RefLookup.deref else PtrLookup.deref;

        pub fn init(
            allocator: Allocator,
            element_mask: MaskInt,
            elements: []const T,
        ) !Self {
            return initMeta(allocator, element_mask, 0, elements);
        }
        pub fn initMeta(
            allocator: Allocator,
            element_mask: MaskInt,
            meta: MetaInt,
            elements: []const T,
        ) !Self {
            return initMetaExtra(allocator, element_mask, meta, elements, {});
        }
        pub fn initMetaExtra(
            allocator: Allocator,
            element_mask: MaskInt,
            meta: MetaInt,
            elements: []const T,
            comptime extra_value: anytype,
        ) !Self {
            const Extra = @TypeOf(extra_value);
            const use_extra = @sizeOf(Extra) > 0;

            std.debug.assert(@popCount(element_mask) == elements.len);
            const nodes = elements.len +% 1;
            const ref = try allocExtra(allocator, nodes, Extra);
            const head = maybeLookup(allocator, ref);
            head.* = .{ .mask = element_mask, .meta = meta };

            const sa = SA{ .head = head };
            if (comptime use_extra) sa.extra(Extra).* = extra_value;

            const data = sa.slice();
            for (data, elements) |*dest, element| dest.* = element;
            return .{ .ref = ref };
        }

        inline fn maybeLookup(allocator: Allocator, ref: Ref) *Head {
            var self = Self{ .ref = ref };
            return self.deref(allocator).head;
        }

        pub fn deinit(self: Self, allocator: Allocator) void {
            self.deinitExtra(allocator, void);
        }
        pub fn deinitExtra(self: Self, allocator: Allocator, comptime Extra: type) void {
            if (comptime is_indexed) {
                allocator.free(self.ref);
            } else {
                const sa = self.lookup();
                const data: [*]align(alignment) T = @ptrCast(sa.head);
                const slice = data[0..totalNodes(sa.size(), Extra)];
                allocator.free(slice);
            }
        }

        /// This is a Handle to the dereferenced data.
        ///
        /// This handle is meant for temporary use, especially for indexed data structures it shouldn't be kept,
        /// instead store the `ref` and use it to lookup this handle/pointer, otherwise you won't be able to use
        /// the data-structure in a position independent manner.
        pub const SA = struct {
            head: *Head,

            pub fn size(self: SA) usize {
                return self.head.size();
            }
            pub fn count(self: SA) usize {
                return self.head.count();
            }
            pub fn empty(self: SA) usize {
                return self.head.empty();
            }
            pub fn slice(self: SA) []align(alignment) T {
                const data: [*]align(alignment) T = @ptrCast(self.head);
                return data[1..self.size()];
            }
            pub fn extra(self: SA, comptime Extra: type) *Extra {
                const address = @intFromPtr(self.head) + extraStart(self.size(), Extra);
                return @ptrFromInt(address);
            }

            pub fn format(
                self: anytype,
                comptime _: []const u8,
                _: std.fmt.FormatOptions,
                writer: anytype,
            ) !void {
                const h = self.head;
                const str_max_size = std.fmt.comptimePrint("{d}", .{max_size});
                if (h.meta == 0) {
                    try writer.print("StencilArray({}, {}) 0b{b:0>" ++ str_max_size ++ "} {any}", .{ max_size, T, h.mask, self.slice() });
                } else {
                    try writer.print("StencilArray({}, {}) 0b{b:0>" ++ str_max_size ++ "} {d} {any}", .{ max_size, T, h.mask, h.meta, self.slice() });
                }
            }

            // the least significant 1 bit in the mask is the first entry in the array
            // the most significant is the last entry in the array
            pub fn bitwiseRef(self: SA, bit_val: MaskInt) T {
                std.debug.assert(@popCount(bit_val) == 1);
                const i = index(self.head.mask, bit_val);
                std.debug.assert(i < self.count());
                return self.slice()[i];
            }
            pub fn bitwiseSet(self: SA, bit_val: MaskInt, val: T) void {
                std.debug.assert(@popCount(bit_val) == 1);
                const i = index(self.head.mask, bit_val);
                std.debug.assert(i < self.count());
                self.slice()[i] = val;
            }
            pub fn bitwisePtr(self: SA, bit_val: MaskInt) *T {
                std.debug.assert(@popCount(bit_val) == 1);
                const i = index(self.head.mask, bit_val);
                std.debug.assert(i < self.count());
                return &self.slice()[i];
            }
            inline fn index(mask: MaskInt, bit_val: MaskInt) u16 {
                return @popCount(mask & (bit_val -% 1));
            }

            fn firstBitMask(m: MaskInt) MaskInt {
                const shift: MaskShift = @intCast(@ctz(m));
                return @as(MaskInt, 1) << shift;
            }
            pub fn update(
                self: SA,
                allocator: Allocator,
                remove_mask: MaskInt,
                add_mask: MaskInt,
                elements: []const T,
            ) !Self {
                return self.updateExtra(allocator, remove_mask, add_mask, elements, void);
            }
            pub fn updateExtra(
                self: SA,
                allocator: Allocator,
                remove_mask: MaskInt,
                add_mask: MaskInt,
                elements: []const T,
                comptime Extra: type,
            ) !Self {
                std.debug.assert(@popCount(add_mask) == elements.len);
                const old_mask = self.head.mask;
                std.debug.assert(remove_mask == remove_mask & old_mask);
                const removed_mask = old_mask ^ remove_mask;
                std.debug.assert(add_mask == (removed_mask ^ add_mask) & add_mask);
                const added_mask = removed_mask | add_mask;

                const old = self.slice();
                const nodes_amount = @popCount(added_mask) + 1;
                const ref = try allocExtra(allocator, nodes_amount, Extra);
                const head = maybeLookup(allocator, ref);
                head.* = .{ .mask = added_mask, .meta = self.head.meta };

                var remaining_mask = added_mask;
                const sa = SA{ .head = head };
                const data = sa.slice();
                for (data) |*dest| {
                    const current = firstBitMask(remaining_mask);
                    if (current & add_mask != 0) {
                        dest.* = elements[index(add_mask, current)];
                    } else if (current & removed_mask != 0) {
                        dest.* = old[index(old_mask, current)];
                    }
                    remaining_mask = remaining_mask ^ current;
                }
                return .{ .ref = ref };
            }

            /// returns an instance with identical content
            /// if the allocator is different the self is of the corresponding type
            pub fn dupe(self: SA, allocator: anytype) !otherSelf(@TypeOf(allocator)) {
                return self.dupeExtra(allocator, void);
            }
            /// returns an instance with identical content
            /// if the allocator is different the self is of the corresponding type,
            /// including an extra value
            pub fn dupeExtra(self: SA, allocator: anytype, comptime Extra: type) !otherSelf(@TypeOf(allocator)) {
                const Other = other(@TypeOf(allocator));

                const old_nodes = self.size();
                const ref = try Other.allocExtra(allocator, old_nodes, Extra);

                var o = Other{ .ref = ref };
                const l = o.deref(allocator);
                const head = l.head;

                head.* = self.head.*;

                const old = self.slice();
                const new = l.slice();
                @memcpy(new, old);

                l.extra(Extra).* = self.extra(Extra).*;
                return .{ .ref = ref };
            }
        };
    };
}

pub fn CheckAndCalculate(comptime max_size: u16, comptime T: type) type {
    return struct {
        pub const MaskInt = std.meta.Int(.unsigned, max_size);
        const shift = std.math.log2_int_ceil(@TypeOf(max_size), max_size);
        pub const MaskShift = std.meta.Int(.unsigned, shift);
        pub const node_size = @max(@alignOf(MaskInt), @sizeOf(T));
        const node_bits = node_size * 8;
        comptime {
            if (node_bits < max_size) {
                @compileLog("max_size", max_size);
                @compileLog("bit count", node_bits);
                @compileError("max_size has to be <= bit count of T " ++ @typeName(T));
            }
        }
        pub const MetaBits = node_bits - max_size;
        pub const MetaInt = std.meta.Int(.unsigned, MetaBits);
    };
}

test StencilArray {
    const t = std.testing;

    const SA1 = StencilArray(8, u32, std.mem.Allocator);
    try t.expectEqual(u8, SA1.MaskInt);
    try t.expectEqual(24, SA1.MetaBits);
    try t.expectEqual(u24, SA1.MetaInt);

    var sa = try SA1.init(t.allocator, 0b10101, &.{ 1, 2, 3 });
    defer sa.deinit(t.allocator);

    const l1 = sa.lookup();
    try t.expectEqual(@as(usize, 3), l1.count());

    l1.head.meta = 35;
    const cs = l1.slice();
    try t.expectEqual(@as(u32, 1), cs[0]);
    try t.expectEqual(@as(u32, 2), cs[1]);
    try t.expectEqual(@as(u32, 3), cs[2]);

    var s = l1.slice();
    s[1] = 22;
    try t.expectEqual(@as(u32, 22), cs[1]);
    s[2] = 33;
    try t.expectEqual(@as(u32, 33), cs[2]);

    try t.expectEqual(@as(u32, 1), l1.bitwiseRef(0b00001));
    try t.expectEqual(@as(u32, 22), l1.bitwiseRef(0b00100));
    try t.expectEqual(@as(u32, 33), l1.bitwiseRef(0b10000));

    var sa2 = try l1.update(t.allocator, 0b100, 0b1010, &.{ 21, 23 });
    defer sa2.deinit(t.allocator);
    const s2 = sa2.lookup().slice();
    try t.expectEqual(@as(u32, 1), s2[0]);
    try t.expectEqual(@as(u32, 21), s2[1]);
    try t.expectEqual(@as(u32, 23), s2[2]);
    try t.expectEqual(@as(u32, 33), s2[3]);

    const BigExtraData = struct {
        some_id: u64,
        another_id: u64,
        a: f64,
        b: f64,
        c: f64,
    };

    const big = BigExtraData{
        .some_id = 4,
        .another_id = 23232,
        .a = -789234.0032,
        .b = 0.333,
        .c = 24.257,
    };
    var sa3 = try SA1.initMetaExtra(t.allocator, 0b10101, 0, &.{ 1, 2, 3 }, big);
    defer sa3.deinitExtra(t.allocator, BigExtraData);

    const l3 = sa3.lookup();
    const s3 = l3.slice();
    try t.expectEqual(1, s3[0]);
    try t.expectEqual(2, s3[1]);
    try t.expectEqual(3, s3[2]);
    try t.expectEqualDeep(big, sa3.lookup().extra(BigExtraData).*);

    const big_size = SA1.totalNodes(0, BigExtraData);
    try t.expectEqual(10, big_size);
    try t.expectEqual(1, SA1.totalNodes(0, u8));

    const specificsizes = @import("specificsizes.zig");
    const Pool = specificsizes.Pool(4, .{});
    var pool = Pool.init(t.allocator);
    defer pool.deinit();

    var sa4 = try l3.dupeExtra(&pool, BigExtraData);
    defer sa4.deinitExtra(&pool, BigExtraData);

    const l4 = sa4.lookup(&pool);
    const s4 = l4.slice();
    try t.expectEqual(1, s4[0]);
    try t.expectEqual(2, s4[1]);
    try t.expectEqual(3, s4[2]);
    try t.expectEqual(10, @TypeOf(sa4).totalNodes(0, BigExtraData));
}

fn example(alloc: anytype) !void {
    const Stuff = StencilArray(8, u32, @TypeOf(alloc));
    std.debug.print("MaskInt: {}\n", .{Stuff.MaskInt});
    std.debug.print("MetaBits: {d}\n", .{Stuff.MetaBits});

    var stuff = try Stuff.init(alloc, 0b10101, &.{ 1, 2, 3 });
    defer stuff.deinit(alloc);

    const l1 = stuff.deref(alloc);
    l1.head.meta = 35;

    const s = l1.slice();
    std.debug.print("0 {}\n", .{s[0]});
    std.debug.print("1 {}\n", .{s[1]});
    std.debug.print("2 {}\n", .{s[2]});
    // std.debug.print("{}\n", .{@as(*const Stuff, stuff)});
    std.debug.print("{}\n", .{l1});

    std.debug.print(".bitwiseRef(0b00001) {}\n", .{l1.bitwiseRef(0b00001)});
    std.debug.print(".bitwiseRef(0b00100) {}\n", .{l1.bitwiseRef(0b00100)});
    std.debug.print(".bitwiseRef(0b10000) {}\n", .{l1.bitwiseRef(0b10000)});

    var stuff2 = try l1.update(alloc, 0b100, 0b1010, &.{ 31, 33 });
    defer stuff2.deinit(alloc);
    const l2 = stuff2.deref(alloc);
    std.debug.print("{}\n", .{l2});
    std.debug.print(".bitwiseRef(0b10000) {}\n\n", .{l2.bitwiseRef(0b10000)});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        switch (gpa.deinit()) {
            .leak => @panic("leaked memory"),
            else => {},
        }
    }

    // ptr usage
    try example(allocator);

    const specificsizes = @import("specificsizes.zig");
    const Pool = specificsizes.Pool(CheckAndCalculate(8, u32).node_size, .{});
    var pool = Pool.init(allocator);
    defer pool.deinit();

    // index usage
    try example(&pool);

    const Options = enum(u2) { A, B, C, D };
    const SA = StencilArray(8, Options, std.mem.Allocator);
    const sa1 = try SA.init(allocator, 0b10101, &.{ .A, .C, .B });
    defer sa1.deinit(allocator);

    const Normal = packed struct {
        group: u16,
        index: u16,
    };
    const SA2 = StencilArray(8, Normal, std.mem.Allocator);
    const sa2 = try SA2.init(allocator, 0b1, &.{.{ .group = 5, .index = 7 }});
    defer sa2.deinit(allocator);

    std.debug.print("MaskInt: {}\n", .{SA2.MaskInt});
    std.debug.print("MetaBits: {d}\n", .{SA2.MetaBits});
}

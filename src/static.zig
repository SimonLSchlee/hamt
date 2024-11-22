//! Simplified versions of the `HAMT` and `StencilArray` that work at comptime without allocators.
const std = @import("std");
const contexts = @import("context.zig");

pub fn StencilArray(comptime E: type) type {
    return struct {
        pub const Element = E;
        pub const Elements = []const E;
        pub const Mask = usize;
        pub const MaskShift = res: {
            const max_size: u16 = @bitSizeOf(Mask);
            const shift = std.math.log2_int_ceil(@TypeOf(max_size), max_size);
            break :res std.meta.Int(.unsigned, shift);
        };

        const Self = @This();
        pub const empty: Self = .{
            .mask = 0,
            .data = &.{},
        };

        mask: Mask,
        data: Elements,

        pub fn update(self: Self, remove_mask: Mask, add_mask: Mask, elements: Elements) Self {
            std.debug.assert(@popCount(add_mask) == elements.len);
            const old_mask = self.mask;
            std.debug.assert(remove_mask == remove_mask & old_mask);
            const removed_mask = old_mask ^ remove_mask;
            std.debug.assert(add_mask == (removed_mask ^ add_mask) & add_mask);
            const added_mask = removed_mask | add_mask;

            const old = self.data;
            const nodes_amount = @popCount(added_mask);
            var data: [nodes_amount]E = undefined;

            var remaining_mask = added_mask;
            for (&data) |*dest| {
                const current = firstBit(remaining_mask);
                if (setIn(current, add_mask)) {
                    dest.* = elements[index(add_mask, current)];
                } else if (setIn(current, removed_mask)) {
                    dest.* = old[index(old_mask, current)];
                }
                remaining_mask = remaining_mask ^ current;
            }

            const frozen: Elements = &data;
            return .{
                .mask = added_mask,
                .data = frozen,
            };
        }

        pub fn bitwiseRef(self: Self, bit_val: Mask) Element {
            std.debug.assert(@popCount(bit_val) == 1);
            const i = index(self.mask, bit_val);
            std.debug.assert(i < self.data.len);
            return self.data[i];
        }
        pub fn get(self: Self, bit_val: Mask) ?Element {
            if (setIn(self.mask, bit_val)) {
                return bitwiseRef(self, bit_val);
            }
            return null;
        }

        fn firstBit(m: Mask) Mask {
            const shift: MaskShift = @intCast(@ctz(m));
            return @as(Mask, 1) << shift;
        }
        inline fn index(mask: Mask, bit_val: Mask) u16 {
            return @popCount(mask & (bit_val -% 1));
        }
        inline fn setIn(current: Mask, mask: Mask) bool {
            return (current & mask) != 0;
        }
    };
}

test StencilArray {
    const Numbers = StencilArray(u32);
    const freeze = comptime blk: {
        var st = Numbers.empty;
        st = st.update(0, 0b1001001, &.{ 1, 4, 5 });
        st = st.update(0, 0b0010100, &.{ 31, 32 });
        break :blk st;
    };

    const t = std.testing;
    try t.expectEqual(1, freeze.data[0]);
    try t.expectEqual(31, freeze.data[1]);
    try t.expectEqual(4, freeze.data[2]);
    try t.expectEqual(32, freeze.data[3]);
    try t.expectEqual(5, freeze.data[4]);
}

const calc = struct {
    const Mask = usize;
    const max_size = @bitSizeOf(Mask);
    const shift = std.math.log2_int_ceil(@TypeOf(max_size), max_size);
    const MaskShift = std.meta.Int(.unsigned, shift);

    const branch_factor = pow2BranchFactor(max_size);
    const consume_bits = pow2Bits(branch_factor);
    const Chunk = std.meta.Int(.unsigned, consume_bits);

    fn pow2Bits(branches: u16) u16 {
        return std.math.log2_int(u16, branches);
    }
    fn pow2BranchFactor(b: u16) u16 {
        return 1 << pow2Bits(b);
    }
    fn maskFromChunk(chunk: Chunk) Mask {
        const bit: Mask = 0b1;
        return bit << chunk;
    }
};

/// A simplified HAMT implementation that works at comptime.
pub fn HAMT(comptime K: type, comptime V: type) type {
    return struct {
        pub const Key = K;
        pub const Value = V;
        pub const KV = struct { K, V };
        pub const KVs = []const KV;

        const Context = contexts.HashContext(K);
        const Chunk = calc.Chunk;
        const Mask = calc.Mask;

        const Self = @This();
        pub const Node = struct {
            pub const Children = StencilArray(*const Node);
            pub const Data = StencilArray(KV);

            children: Children,
            data: Data,

            pub const empty: Node = .{
                .children = Children.empty,
                .data = Data.empty,
            };
        };

        root: *const Node,

        pub const empty: Self = .{
            .root = &Node.empty,
        };

        /// Adds a key value pair into the HAMT needs to be called at comptime.
        pub fn put(self: *Self, key: K, value: V) void {
            const ctx: Context = undefined;
            var iter = ctx.prefix(Chunk, key);
            contexts.iterSetup(&iter);
            if (comptime putWalk(key, value, &iter, 0, self.root)) |new_node| {
                self.root = new_node;
            }
        }

        fn putWalk(key: K, value: V, iter: anytype, chunk_i: u32, parent: *const Node) ?*const Node {
            const ctx: Context = undefined;

            const chunk = iter.next() orelse return null;

            const current = parent;
            const new_mask = calc.maskFromChunk(chunk);
            if (current.children.get(new_mask)) |new_parent| {
                const child_node = putWalk(
                    key,
                    value,
                    iter,
                    chunk_i + 1,
                    new_parent,
                ) orelse return null;

                const old_mask = current.children.mask;
                const remove_mask = old_mask & new_mask;
                return &Node{
                    .children = current.children.update(remove_mask, new_mask, &.{child_node}),
                    .data = current.data,
                };
            }

            if (current.data.get(new_mask)) |old_data| {
                const old_key = old_data[0];
                if (ctx.eql(old_key, key)) {
                    const old_value = old_data[1];
                    if (std.meta.eql(old_value, value)) return null;
                    return &Node{
                        .children = current.children,
                        .data = current.data.update(new_mask, new_mask, &.{.{ key, value }}),
                    };
                }

                const next_chunk_i = chunk_i + 1;
                const pair_mask = calculate_child_pair_mask(old_key, next_chunk_i);
                const new_child = &Node{
                    .children = Node.Children.empty,
                    .data = Node.Data.empty.update(0, pair_mask, &.{old_data}),
                };

                const new_child2 = putWalk(key, value, iter, next_chunk_i, new_child).?;
                return &Node{
                    .children = current.children.update(0, new_mask, &.{new_child2}),
                    .data = current.data.update(new_mask, 0, &.{}),
                };
            }

            // neither child nor key existed so just add it
            return &Node{
                .children = current.children,
                .data = current.data.update(0, new_mask, &.{.{ key, value }}),
            };
        }

        /// Retrieves a value given a key, or returns null if it doesn't exist.
        pub fn get(self: *const Self, key: K) ?V {
            const ctx: Context = undefined;

            var current = self.root;
            var iter = ctx.prefix(Chunk, key);
            contexts.iterSetup(&iter);
            while (iter.next()) |chunk| {
                const mask = calc.maskFromChunk(chunk);
                if (current.children.get(mask)) |new_current| {
                    current = new_current;
                    continue;
                }
                if (current.data.get(mask)) |kv| {
                    const old_key = kv[0];
                    if (ctx.eql(old_key, key)) return kv[1];
                }
                return null;
            }
            return null;
        }

        /// Call this function to convert the HAMT to a value
        /// that can be queried at runtime. Converts the comptime variable
        /// version into a frozen version that no longer references comptime vars.
        pub fn freeze(self: Self) Self {
            return .{
                .root = copyNode(self.root),
            };
        }

        fn copyNode(comptime n: *const Node) *const Node {
            var children: [n.children.data.len]*const Node = undefined;
            var data: [n.data.data.len]KV = undefined;
            for (n.children.data, 0..) |child, i| children[i] = copyNode(child);
            for (n.data.data, 0..) |d, i| data[i] = d;
            const frozen_children = children;
            const frozen_data = data;
            return &Node{
                .children = .{ .mask = n.children.mask, .data = &frozen_children },
                .data = .{ .mask = n.data.mask, .data = &frozen_data },
            };
        }

        fn calculate_child_pair_mask(old_key: K, next_chunk_i: u32) Mask {
            const ctx: Context = undefined;
            var iter = ctx.prefix(Chunk, old_key);
            contexts.iterSetup(&iter);
            var chunk_i: u32 = 0;
            while (iter.next()) |chunk| {
                if (chunk_i == next_chunk_i) return calc.maskFromChunk(chunk);
                chunk_i += 1;
            }
            comptime unreachable;
        }

        // TODO maybe create a flattened version/support datastructure that
        // allows for the creation of an easy iterator
        // that doesn't need a lot of state or recomputation?
    };
}

test HAMT {
    const t = std.testing;
    const Map = HAMT(u32, []const u8);

    const frozen = comptime blk: {
        var map = Map.empty;
        map.put(35, "hello");
        map.put(77, "world");
        map.put(17, "foo");
        break :blk map.freeze();
    };

    try t.expectEqualSlices(u8, "hello", frozen.get(35).?);
    try t.expectEqualSlices(u8, "world", frozen.get(77).?);
    try t.expectEqualSlices(u8, "foo", frozen.get(17).?);
    try t.expectEqual(@as(?[]const u8, null), frozen.get(42));
}

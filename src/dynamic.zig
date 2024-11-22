//! A Hash Array Mapped Trie implementation based on:
//! - Ideal Hash Trees (Bagwell, Phil  2001)
//!   https://infoscience.epfl.ch/record/64398
//!
//! using Stencilvectors as described in:
//! - Runtime and Compiler Support for HAMTs
//!   https://www-old.cs.utah.edu/plt/publications/dls21-tzf.pdf
//!
//! Take a look at `HAMT` and `StencilArray` for usage examples.
//!
//! Both data structures can be used in imperative and persistent ways, depending on your needs.

const std = @import("std");
const custom = @import("customallocator.zig");
const stencilarray = @import("stencilarray.zig");
pub const StencilArray = stencilarray.StencilArray;

const specificsizes = @import("specificsizes.zig");
const contexts = @import("context.zig");

fn isOptional(comptime V: type) bool {
    return switch (@typeInfo(V)) {
        .Optional => true,
        else => false,
    };
}

fn StripOptional(comptime V: type) type {
    return switch (@typeInfo(V)) {
        .Optional => |o| o.child,
        else => V,
    };
}

fn nonZeroFields(comptime T: type) comptime_int {
    var counter = 0;
    for (std.meta.fields(T)) |f| {
        if (@sizeOf(f.type) > 0) counter += 1;
    }
    return counter;
}
fn pow2Bits(branches: u16) u16 {
    return std.math.log2_int(u16, branches);
}
fn pow2BranchFactor(b: u16) u16 {
    return 1 << pow2Bits(b);
}

/// Calculates the specific custom allocator type based on the element size.
pub fn poolAllocator(comptime ElementSize: usize) Options.CustomAllocatorInfo {
    const min_size = @sizeOf(specificsizes.SlabIndex);
    const base_size = @max(min_size, ElementSize);
    return .{
        .allocator = *specificsizes.Pool(base_size, .{}),
        .base_size = base_size,
    };
}

/// The options supported by the HAMT.
pub const Options = struct {
    pub const Style = enum {
        /// Old nodes are deallocated.
        imperative,
        /// Old nodes aren't deallocated, this means the old versions stay valid and are still
        /// accessable through old references. Use an arena or pool to deallocated all nodes at once.
        /// Or manually deallocate specific nodes through traversal.
        persistent,
    };
    pub const Reference = enum {
        /// Automatically choose
        auto,
        /// Nodes are referenced via pointers, resolving references is easy,
        /// data structure isn't position independent.
        pointer,
        /// Nodes are referenced with indizies, this index is resolved via the `lookup` function of the custom allocator,
        /// data structure is position independent.
        index,
    };

    /// If you use `.persistent` you probably want to use an arena (or something similar) or a custom allocator for allocation,
    /// because the persistent version allows you to share nodes and keep pointers to older versions.
    /// Thus you need a way to deallocate old nodes. With `.imperative` old nodes get deallocated immediately.
    style: Style = .imperative,
    /// This specifies the type that is required for counts.
    /// The count type could be larger but not smaller, depending on whats available.
    /// Setting this to void means the count gets calculated through a tree walk on each access.
    min_required_counter: type = u32,
    // Specify how references are implemented, default is to choose something
    /// that saves memory if automatically possible.
    ref: Reference = .auto,
    /// The context is required to have a `prefix` function and an `eql` function.
    /// If reference type is `Index` it also needs to have an `index` function.
    context: ?type = null,
    /// Whether the key is hashed before it is used. Only set this to false if you really want it
    /// for example when you know that the key has a good distribution.
    hashing: bool = true,
    // Hint about how you could improve e.g. by providing `ref` and `deref` functions,
    // so that you can use indizies as references.
    // hint: bool = false,

    /// This can be used to provide a custom allocator, use void to disable
    pointer_allocator: type = std.mem.Allocator,

    /// This can be used to provide a custom allocator, use void to disable
    index_allocator: ?CustomAllocatorInfo = null,

    pub const CustomAllocatorInfo = struct {
        allocator: type,
        base_size: usize,
    };
    pub const CustomAllocatorFn = fn (comptime usize) CustomAllocatorInfo;
    fn getCustomAllocator(comptime K: type, comptime V: type, customFn: CustomAllocatorFn) CustomAllocatorInfo {
        return customFn(@max(@sizeOf(K), @sizeOf(V)));
    }

    pub fn withCustomAllocator(self: Options, comptime K: type, comptime V: type, customFn: CustomAllocatorFn) Options {
        return .{
            .style = self.style,
            .min_required_counter = self.min_required_counter,
            .ref = self.ref,
            .context = self.context,
            .hashing = self.hashing,
            // .hint = self.hint,
            .pointer_allocator = self.pointer_allocator,
            .index_allocator = getCustomAllocator(K, V, customFn),
        };
    }
    const DummyCustomAllocator = struct {
        pub fn init(_: std.mem.Allocator) DummyCustomAllocator {
            return .{};
        }
        pub fn deinit(_: *DummyCustomAllocator) void {}
    };
    pub fn customAllocator(self: Options) type {
        if (self.index_allocator) |info| return std.meta.Child(info.allocator);
        return DummyCustomAllocator;
    }
    pub fn indexedElementSize(self: Options, comptime K: type, comptime V: type) comptime_int {
        if (self.index_allocator) |info|
            return @max(@sizeOf(K), @sizeOf(V), info.base_size);
        @compileError("no custom allocator provided via .index_allocator");
    }

    pub fn config(options: Options, comptime K: type, comptime V: type) Config {
        const context = if (options.hashing) contexts.HashContext(K) else contexts.PrefixContext(K);

        const enabled_pointer = options.pointer_allocator != void;
        if (enabled_pointer and custom.isRefAllocator(options.pointer_allocator)) {
            @compileError("optionsToConfig: .pointer_allocator must not be a refAllocator, but got: " ++ @typeName(options.pointer_allocator));
        }
        if (options.ref == .pointer and !enabled_pointer) {
            @compileError("optionsToConfig: ref is .pointer but no pointer allocator was provided: " ++ @typeName(options.pointer_allocator));
        }

        const enabled_index = options.index_allocator != null;
        if (enabled_index and !custom.isRefAllocator(options.index_allocator.?.allocator)) {
            @compileError("optionsToConfig: .index_allocator must be a refAllocator, but got: " ++ @typeName(options.index_allocator));
        }
        if (options.ref == .index and !enabled_index) {
            @compileError("optionsToConfig: ref is .index but no index allocator was provided: " ++ @typeName(options.index_allocator));
        }

        const neither = !(enabled_pointer or enabled_index);
        if (neither) {
            @compileError("optionsToConfig: no valid allocator provided:" ++ "\n    .pointer_allocator = " ++ @typeName(options.pointer_allocator) ++ "\n    .index_allocator = " ++ @typeName(options.index_allocator));
        }

        const both = enabled_pointer and enabled_index;
        const is_indexed = both and switch (options.ref) {
            .auto => if (both) options.indexedElementSize(K, V) < @sizeOf(*anyopaque) else false,
            .pointer => false,
            .index => true,
        };

        return .{
            .key = K,
            .value = V,

            .style = options.style,
            .keep_history = switch (options.style) {
                .imperative => false,
                .persistent => true,
            },
            .min_required_counter = options.min_required_counter,
            .is_indexed = is_indexed,
            .context = context,
            .allocator = if (is_indexed) options.index_allocator.?.allocator else options.pointer_allocator,
        };
    }
};

const Config = struct {
    key: type,
    value: type,

    style: Options.Style,
    keep_history: bool,
    min_required_counter: type,

    is_indexed: bool,
    context: type,

    allocator: type,
};

fn canBePacked(comptime T: type) bool {
    if (std.meta.hasUniqueRepresentation(T)) return true;
    return switch (@typeInfo(T)) {
        .Int, .Float => true,
        else => false,
    };
}

/// Calculations is used to calculate a bunch of properties like the size of bitmasks,
/// branching factors etc. those properties are used in the implementation of the HAMT.
pub fn Calculations(comptime K: type, comptime V: type, comptime config: Config) type {
    return struct {
        const keep_history = config.keep_history;
        const imperative = config.style == .Imperative;
        const Context = config.context;
        const Allocator = config.allocator;

        pub const is_indexed = config.is_indexed;
        const Ref = if (is_indexed) custom.LookupFunctionGetReference(Allocator) else *anyopaque;

        // When the value is optional we use the stencilarray to avoid
        // storing the value. So we use a bit for the key and another bit
        // for whether the value is present.
        // When the value isn't optional store key and value as a pair/struct.
        // If key and value together have double the size of the ref, it may make
        // sense to not group key and value
        // Basically key and value should only be grouped if that doesn't increase the size of node

        const Val = StripOptional(V);

        const Pair = if (canBePacked(K) and canBePacked(V)) packed struct {
            key: K,
            value: Val,
        } else struct {
            key: K,
            value: Val,
        };
        const grouped = struct {
            const masks = 2;
            const size = @max(@sizeOf(Pair), @sizeOf(Ref));
            const alignment = @max(@alignOf(Pair), @alignOf(Ref));
            data: [size]u8 align(alignment),
            fn getPair(e: grouped) Pair {
                const pair: *const Pair = @ptrCast(@alignCast(&e.data));
                return pair.*;
            }

            fn getChild(e: grouped) Ref {
                const ref: *const Ref = @ptrCast(@alignCast(&e.data));
                return ref.*;
            }
            fn getKey(e: grouped) K {
                return e.getPair().key;
            }
            fn getValue(e: grouped) V {
                return e.getPair().value;
            }
        };
        const separate = struct {
            const masks = if (@sizeOf(V) > 0) 3 else 2;
            const size = @max(@sizeOf(K), @sizeOf(V), @sizeOf(Ref));
            const alignment = @max(@alignOf(K), @alignOf(V), @alignOf(Ref));
            data: [size]u8 align(alignment),

            fn getChild(e: separate) Ref {
                const ref: *const Ref = @ptrCast(@alignCast(&e.data));
                return ref.*;
            }
            fn getKey(e: separate) K {
                const key: *const K = @ptrCast(@alignCast(&e.data));
                return key.*;
            }
            fn getValue(e: separate) V {
                const value: *const V = @ptrCast(@alignCast(&e.data));
                return value.*;
            }
        };

        // TODO think about this: optimization for bool and other very small value types like
        // u4 or other small-ish enums etc.: pack values in a bitfield
        // for example for bool store it in the meta bits just using a child and key mask
        // only if grouped doesn't work (because key is too big)
        // basically if all values can be packed into a single element of the stencilarray
        // so we could have separate grouped and bitpacked
        // const bitpacked = packed union {
        //     child: Ref,
        //     key: K,
        //     values: [branch_factor]Val,
        // };

        const ElementType = enum { separate, grouped };
        fn SelectElementType() ElementType {
            if (isOptional(V)) return .separate;
            return if (@sizeOf(separate) < @sizeOf(grouped)) .separate else .grouped;
        }
        fn GetElementType() type {
            return switch (SelectElementType()) {
                .separate => separate,
                .grouped => grouped,
            };
        }
        pub const Element = GetElementType();
        const is_separate = SelectElementType() == .separate;

        fn element_pair(key: K, value: Val) Element {
            switch (comptime SelectElementType()) {
                .grouped => {
                    var e: Element = undefined;
                    const p: *Pair = @ptrCast(@alignCast(&e.data));
                    p.key = key;
                    p.value = value;
                    return e;
                },
                .separate => @compileError("separate needs to use element_key and element_value"),
            }
        }
        fn element_child(child: Ref) Element {
            var e: Element = undefined;
            const c: *Ref = @ptrCast(@alignCast(&e.data));
            c.* = child;
            return e;
        }
        fn element_key(key: K) Element {
            var e: Element = undefined;
            const k: *K = @ptrCast(@alignCast(&e.data));
            k.* = key;
            return e;
        }
        fn element_value(value: V) Element {
            var e: Element = undefined;
            const v: *V = @ptrCast(@alignCast(&e.data));
            v.* = value;
            return e;
        }

        inline fn elements_pair(k: K, v: Val) []const Element {
            return switch (comptime SelectElementType()) {
                .grouped => &.{element_pair(k, v)},
                .separate => &.{ element_key(k), element_value(v) },
            };
        }
        inline fn elements_value(k: K, v: Val) []const Element {
            return switch (comptime SelectElementType()) {
                .grouped => &.{element_pair(k, v)},
                .separate => &.{element_value(v)},
            };
        }
        inline fn elements_extract_pair(node: Handle.SA, key_mask: Handle.MaskInt, value_mask: Handle.MaskInt) []const Element {
            return switch (comptime SelectElementType()) {
                .grouped => &.{node.bitwiseRef(key_mask)},
                .separate => &.{ node.bitwiseRef(key_mask), node.bitwiseRef(value_mask) },
            };
        }

        const needed_masks = Element.masks;
        const bits = Element.size * 8;
        const available_bits = @divTrunc(bits, needed_masks);

        const branch_factor = pow2BranchFactor(available_bits);
        const branch_bits = branch_factor * needed_masks;
        const remaining_bits = bits - branch_bits;

        const count_disabled = config.min_required_counter == void;
        const min_counter_bits = if (count_disabled) 0 else @typeInfo(config.min_required_counter).Int.bits;
        const use_meta_as_count = !count_disabled and remaining_bits >= min_counter_bits;
        const total_bits = branch_bits;

        const consume_bits = pow2Bits(branch_factor);
        const Chunk = std.meta.Int(.unsigned, consume_bits);

        const shift_child = 0;
        const shift_key = shift_child + branch_factor;
        const shift_value = if (is_separate) shift_key + branch_factor else shift_key;

        fn get_mask(shift: Handle.MaskShift) Handle.MaskInt {
            const mask = (1 << branch_factor) - 1;
            return mask << shift;
        }
        const mask_child = get_mask(shift_child);
        const mask_key = get_mask(shift_key);
        const mask_value = get_mask(shift_value);
        const mask_child_key = mask_child | mask_key;

        const mask_kind = enum { child, key, value };
        fn maskFromChunk(kind: mask_kind, chunk: Chunk) Handle.MaskInt {
            const bit: Handle.MaskInt = 0b1;
            const base_shift: Handle.MaskShift = switch (kind) {
                .child => shift_child,
                .key => shift_key,
                .value => shift_value,
            };
            const shift: Handle.MaskShift = @intCast(base_shift + chunk);
            return bit << shift;
        }

        const count_bits = @min(bits, @sizeOf(usize) * 8);
        comptime {
            if (!count_disabled) {
                const min_bits = @typeInfo(config.min_required_counter).Int.bits;
                if (count_bits < min_bits) {
                    const msg = std.fmt.comptimePrint("Counter type {s} has {d} bits and doesn't fit into element of {d} bits", .{
                        @typeName(config.min_required_counter),
                        min_bits,
                        @sizeOf(Element) * 8,
                    });
                    @compileError(msg);
                }
            }
        }
        pub const Count = if (count_disabled) usize else std.meta.Int(.unsigned, count_bits);
        const SVOptions = stencilarray.CheckAndCalculate(total_bits, Element);
        const Handle = StencilArray(total_bits, Element, Allocator);

        fn printDiagnostics() void {
            std.debug.print("=================================\n", .{});
            std.debug.print("HAMT diagnostics\n", .{});
            std.debug.print("key: {}\n", .{K});
            std.debug.print("value: {}\n", .{V});
            std.debug.print("element: {}\n", .{Element});
            std.debug.print("grouped/separate: {s}\n", .{if (is_separate) "separate" else "grouped"});
            std.debug.print("reference: {s}\n", .{if (is_indexed) "index" else "pointer"});
            std.debug.print("---------------\n", .{});
            std.debug.print("needed_masks: {d}\n", .{needed_masks});
            std.debug.print("bits: {d}\n", .{bits});
            std.debug.print("available_bits: {d}\n", .{available_bits});
            std.debug.print("\n", .{});
            std.debug.print("branch_factor: {d}\n", .{branch_factor});
            std.debug.print("branch_bits: {d}\n", .{branch_bits});
            std.debug.print("remaining_bits: {d}\n", .{remaining_bits});
            std.debug.print("\n", .{});
            std.debug.print("count_disabled: {}\n", .{count_disabled});
            std.debug.print("use_meta_as_count: {}\n", .{use_meta_as_count});
            std.debug.print("total_bits: {d}\n", .{total_bits});
            std.debug.print("meta bits: {d}\n", .{SVOptions.MetaBits});
            std.debug.print("\n", .{});

            std.debug.print("consume_bits: {d}\n", .{consume_bits});

            if (contexts.getFixedSizeKeyBits(K)) |key_bits| {
                std.debug.print("fixed size key?: true\n", .{});
                std.debug.print("key bits: {d}\n", .{key_bits});

                if (std.math.divCeil(comptime_int, key_bits, consume_bits)) |max_depth| {
                    std.debug.print("tree max depth: {d}\n", .{max_depth});
                } else |err| {
                    std.debug.print("tree max depth: <{s}>\n", .{@errorName(err)});
                }
            } else {
                std.debug.print("fixed size key?: false\n", .{});
            }

            std.debug.print("=================================\n\n", .{});
        }

        fn calculateSizes(allocator: std.mem.Allocator) !void {
            // zero size is not stored via allocator

            const min_size: u16 = 1;
            const count_nodes = if (count_disabled or use_meta_as_count) 0 else 1;

            var sizes = std.AutoHashMap(u16, void).init(allocator);
            defer sizes.deinit();

            for (0..branch_factor) |i| {
                const b: u16 = 1 + @as(u16, @intCast(i));

                if (is_separate) {
                    for (b..2 * b + 1) |j| {
                        const node_size: u16 = @intCast(j);
                        const size = min_size + node_size;
                        try sizes.put(size, {});
                        if (size > 1) try sizes.put(size + count_nodes, {}); // node has children/count
                    }
                } else {
                    const node_size: u16 = b;
                    const size = min_size + node_size;
                    try sizes.put(size, {});
                    if (size > 1) try sizes.put(size + count_nodes, {}); // node has children/count
                }
            }

            var list = std.ArrayList(u16).init(allocator);
            defer list.deinit();

            var iterator = sizes.keyIterator();
            while (iterator.next()) |key| {
                try list.append(key.*);
            }

            std.mem.sort(u16, list.items, {}, std.sort.asc(u16));
            std.debug.print("sizes: {any} length: {}\n", .{ list.items, list.items.len });
        }
    };
}

pub fn HAMTUnmanaged(comptime K: type, comptime V: type, comptime options: Options) type {
    return struct {
        const Self = @This();

        const config = options.config(K, V);
        const keep_history = config.keep_history;
        const imperative = config.style == .Imperative;
        const Context = config.context;
        const Allocator = config.allocator;

        const calc = Calculations(K, V, config);
        const is_separate = calc.is_separate;
        const is_indexed = calc.is_indexed;
        const count_disabled = calc.count_disabled;
        const use_meta_as_count = calc.use_meta_as_count;
        const Count = calc.Count;

        const Element = calc.Element;
        const Chunk = calc.Chunk;
        const Ref = calc.Ref;
        const Handle = calc.Handle;

        const empty_node = Handle.Head.zero;
        const empty_handle = if (is_indexed) Handle{ .ref = zeroRef() } else Handle{ .ref = @ptrCast(@alignCast(@constCast(&empty_node))) };
        const empty_sa = Handle.SA{ .head = @ptrCast(@constCast(&empty_node)) };

        ref: Ref = zeroRef(),

        inline fn zeroRef() Ref {
            return if (comptime is_indexed) .{} else @constCast(&empty_node);
        }

        fn handle(ref: Ref) Handle {
            if (comptime calc.is_indexed) {
                return if (ref.zeroRef()) empty_handle else Handle{ .ref = ref };
            } else {
                return Handle{ .ref = @ptrCast(@alignCast(ref)) };
            }
        }
        fn deref(allocator: Allocator, node: Handle) Handle.SA {
            if (comptime calc.is_indexed) {
                return if (node.ref.zeroRef()) empty_sa else node.deref(allocator);
            } else {
                return node.deref(allocator);
            }
        }

        fn printDiagnostics() void {
            calc.printDiagnostics();
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            if (!comptime keep_history) ref_deinit(self.ref, allocator);
            self.ref = zeroRef();
        }

        fn ref_deinit(ref: Ref, allocator: Allocator) void {
            const node = handle(ref);
            var iter = node_element_iter(allocator, node);
            while (iter.next()) |ne| {
                switch (ne.kind) {
                    .child => ref_deinit(ne.element.getChild(), allocator),
                    else => {},
                }
            }
            refDeinit(ref, allocator);
        }

        pub fn empty(self: Self, allocator: Allocator) bool {
            return self.count(allocator) == 0;
        }

        pub fn count(self: Self, allocator: Allocator) usize {
            return handleCount(handle(self.ref), allocator);
        }

        fn handleCount(h: Handle, allocator: Allocator) Count {
            if (comptime count_disabled) {
                // if the count is disabled it can only be calculated through traversal
                var iter = node_element_iter(allocator, h);
                var sum: Count = 0;
                while (iter.next()) |ne| {
                    switch (ne.kind) {
                        .child => sum += handleCount(handle(ne.element.getChild()), allocator),
                        .key, .pair => sum += 1,
                        else => {},
                    }
                }
                return sum;
            } else {
                // we distinguish between leaf nodes and inner nodes
                // leaf nodes don't have a separate count, their stencilarray mask already represents a count
                // leaf nodes may have count zero in that case it is an empty root node for an empty tree
                // the empty leaf is only allowed as the root node, it can't be child of some other node
                // and it mustn't be modified
                //
                // if it is an inner mode the count is either stored in the MetaInt or in a prefix Count
                const node = deref(allocator, h);
                if (nodeIsLeaf(node)) {
                    return @popCount(node.head.mask & calc.mask_child_key);
                }
                if (comptime use_meta_as_count) {
                    return node.head.meta;
                } else {
                    return node.extra(Count).*;
                }
            }
        }

        fn nodeIsZero(node: Handle.SA) bool {
            return node.count() == 0;
        }
        fn nodeHasChildren(node: Handle.SA) bool {
            return 0 != node.head.mask & calc.mask_child;
        }
        fn nodeIsLeaf(node: Handle.SA) bool {
            return !nodeHasChildren(node);
        }

        fn update_ref_count(allocator: Allocator, node: Handle, delta: i8) void {
            update_node_count(deref(allocator, node), delta);
        }
        fn update_node_count(node: Handle.SA, delta: i8) void {
            if (comptime !count_disabled) {
                if (nodeIsLeaf(node)) return;

                if (comptime use_meta_as_count) {
                    applyDelta(&node.head.meta, delta);
                } else {
                    applyDelta(node.extra(Count), delta);
                }
            }
        }
        fn applyDelta(dest: anytype, delta: i8) void {
            if (delta < 0) dest.* -|= @abs(delta) else dest.* +|= @intCast(delta);
        }

        fn handleSetCount(node: Handle, allocator: Allocator, new_count: Count) void {
            if (comptime !count_disabled) {
                if (comptime use_meta_as_count) {
                    node.deref(allocator).head.meta = @intCast(new_count);
                } else {
                    node.deref(allocator).extra(Count).* = @intCast(new_count);
                }
            }
        }

        fn change_node(
            node: Handle,
            allocator: Allocator,
            remove_mask: Handle.MaskInt,
            add_mask: Handle.MaskInt,
            elements: []const Element,
        ) !Handle {
            const l = deref(allocator, node);
            const next_mask = l.head.mask | add_mask;
            const internal_node = 0 != next_mask & calc.mask_child;

            var new_node: Handle = undefined;
            if (internal_node) {
                const Extra = if (comptime count_disabled or use_meta_as_count) void else Count;
                new_node = try l.updateExtra(allocator, remove_mask, add_mask, elements, Extra);
                if (comptime !count_disabled) handleSetCount(new_node, allocator, handleCount(node, allocator));
            } else {
                new_node = try l.update(allocator, remove_mask, add_mask, elements);
            }

            return new_node;
        }

        fn refDeinit(ref: Ref, allocator: Allocator) void {
            if (comptime is_indexed) {
                if (ref.zeroRef()) return;
                allocator.free(ref);
            } else {
                const node = handle(ref);
                const nsv = node.deref(allocator);
                if (nodeIsZero(nsv)) return;
                if (comptime count_disabled or use_meta_as_count) {
                    node.deinit(allocator);
                } else {
                    if (nodeIsLeaf(nsv)) {
                        node.deinit(allocator);
                    } else {
                        node.deinitExtra(allocator, Count);
                    }
                }
            }
        }

        inline fn used(old_mask: Handle.MaskInt, other: Handle.MaskInt) bool {
            return 0 != (old_mask & other);
        }
        inline fn iterSetup(iter: anytype) void {
            const Iter = std.meta.Child(@TypeOf(iter));
            if (@hasDecl(Iter, "setup")) iter.setup();
        }

        /// Removes a value stored under key from the HAMT.
        pub fn remove(self: *Self, allocator: Allocator, key: K) !void {
            if (@sizeOf(Context) != 0)
                @compileError("Cannot infer context " ++ @typeName(Context) ++ ", call removeContext instead.");
            return self.removeContext(allocator, key, undefined);
        }

        pub fn removeContext(self: *Self, allocator: Allocator, key: K, ctx: Context) !void {
            var iter = ctx.prefix(Chunk, key);
            iterSetup(&iter);
            if (try removeContextWalk(allocator, key, ctx, &iter, self.ref)) |new_node| {
                self.ref = new_node.ref;
            }
        }

        fn removeContextWalk(allocator: Allocator, key: K, ctx: Context, iter: anytype, parent: Ref) !?Handle {
            const chunk = iter.next() orelse return null;

            const current = handle(parent);
            const nsv = deref(allocator, current);
            const old_mask = nsv.head.mask;

            const child_mask = calc.maskFromChunk(.child, chunk);
            if (used(old_mask, child_mask)) {
                const child_node = try removeContextWalk(
                    allocator,
                    key,
                    ctx,
                    iter,
                    nsv.bitwiseRef(child_mask).getChild(),
                ) orelse return null;

                const remove_mask = old_mask & child_mask;
                const new_node = try change_node(current, allocator, remove_mask, child_mask, &.{
                    calc.element_child(child_node.ref),
                });
                update_ref_count(allocator, new_node, -1);

                if (comptime !keep_history) refDeinit(parent, allocator);
                return new_node;
            }

            const key_mask = calc.maskFromChunk(.key, chunk);
            if (used(old_mask, key_mask)) {
                const old_key = nsv.bitwiseRef(key_mask).getKey();
                if (ctx.eql(old_key, key)) {
                    const value_mask = calc.maskFromChunk(.value, chunk);
                    const pair_mask = key_mask | value_mask;
                    const new_ref = try change_node(current, allocator, old_mask & pair_mask, 0, &.{});

                    if (comptime !keep_history) refDeinit(parent, allocator);
                    return new_ref;
                }
            }

            // no matching key found, we are done
            return null;
        }

        /// Adds a value associated with key to the HAMT. If the pair of key and value already exists the HAMT remains unchanged.
        pub fn put(self: *Self, allocator: Allocator, key: K, value: V) !void {
            if (@sizeOf(Context) != 0)
                @compileError("Cannot infer context " ++ @typeName(Context) ++ ", call putContext instead.");
            return self.putContext(allocator, key, value, undefined);
        }

        pub fn putContext(self: *Self, allocator: Allocator, key: K, value: V, ctx: Context) !void {
            var iter = ctx.prefix(Chunk, key);
            iterSetup(&iter);
            var increment_count: bool = false;
            if (try putContextWalk(allocator, key, value, ctx, &iter, &increment_count, 0, self.ref)) |new_node| {
                self.ref = new_node.ref;
            }
        }

        pub fn putContextWalk(allocator: Allocator, key: K, value: V, ctx: Context, iter: anytype, increment_count: *bool, chunk_i: u32, parent: Ref) !?Handle {
            const chunk = iter.next() orelse return null;

            const current = handle(parent);
            const nsv = deref(allocator, current);
            const old_mask = nsv.head.mask;

            const child_mask = calc.maskFromChunk(.child, chunk);
            if (used(old_mask, child_mask)) {
                const child_node = try putContextWalk(
                    allocator,
                    key,
                    value,
                    ctx,
                    iter,
                    increment_count,
                    chunk_i + 1,
                    nsv.bitwiseRef(child_mask).getChild(),
                ) orelse return null;

                const remove_mask = old_mask & child_mask;
                const new_node = try change_node(current, allocator, remove_mask, child_mask, &.{
                    calc.element_child(child_node.ref),
                });
                if (increment_count.*) update_ref_count(allocator, new_node, 1);
                if (comptime !keep_history) refDeinit(parent, allocator);
                return new_node;
            }

            const key_mask = calc.maskFromChunk(.key, chunk);
            const value_mask = calc.maskFromChunk(.value, chunk);
            if (used(old_mask, key_mask)) {
                const old_key = nsv.bitwiseRef(key_mask).getKey();
                if (ctx.eql(old_key, key)) {
                    const old_value = nsv.bitwiseRef(value_mask).getValue();
                    if (std.meta.eql(old_value, value)) return null; // value is already set

                    const new_node = try change_node(current, allocator, old_mask & value_mask, value_mask, calc.elements_value(key, value));

                    if (comptime !keep_history) refDeinit(parent, allocator);
                    return new_node;
                }
                // } else {

                const old_pair = calc.elements_extract_pair(nsv, key_mask, value_mask);
                const next_chunk_i = chunk_i + 1;
                const pair_mask = try calculate_child_pair_mask(old_key, next_chunk_i, ctx);
                const new_child = try change_node(empty_handle, allocator, 0, pair_mask, old_pair);
                errdefer refDeinit(new_child.ref, allocator);

                const maybe_child = try putContextWalk(
                    allocator,
                    key,
                    value,
                    ctx,
                    iter,
                    increment_count,
                    next_chunk_i,
                    new_child.ref,
                );
                const new_child2 = maybe_child.?;

                const mask = key_mask | value_mask;
                const remove_mask = old_mask & mask;

                // else remove key and value and add it to a new child
                const new_node = try change_node(current, allocator, remove_mask, child_mask, &.{
                    calc.element_child(new_child2.ref),
                });
                if (increment_count.*) update_ref_count(allocator, new_node, 1);
                if (comptime !keep_history) refDeinit(parent, allocator);

                return new_node;
            }

            // we have reached the node where we can insert the value
            const mask = key_mask | value_mask;
            const new_node = try change_node(current, allocator, old_mask & mask, mask, calc.elements_pair(key, value));
            if (comptime !keep_history) refDeinit(parent, allocator);
            update_ref_count(allocator, new_node, 1);
            increment_count.* = true;
            return new_node;
        }

        fn calculate_child_pair_mask(old_key: K, next_chunk_i: u32, ctx: Context) !Handle.MaskInt {
            var iter = ctx.prefix(Chunk, old_key);
            iterSetup(&iter);
            var chunk_i: u32 = 0;
            while (iter.next()) |chunk| {
                if (chunk_i == next_chunk_i) {
                    const key_mask = calc.maskFromChunk(.key, chunk);
                    const value_mask = calc.maskFromChunk(.value, chunk);
                    const pair_mask = key_mask | value_mask;
                    return pair_mask;
                }
                chunk_i += 1;
            }
            return error.PrefixChunkExhausted;
        }

        const NodeElement = struct {
            const Kind = enum { child, key, value, pair };
            kind: Kind,
            element: Element,
        };
        const NodeElementIterator = struct {
            allocator: Allocator,
            node: Handle,
            current: usize = 0,

            pub fn next(it: *NodeElementIterator) ?NodeElement {
                const nsv = deref(it.allocator, it.node);
                const old_mask = nsv.head.mask;

                const index = calc.shift_child + it.current;
                if (index >= nsv.count()) return null;

                const children = @popCount(old_mask & calc.mask_child);
                const keys_index = calc.shift_child + children;

                var kind = NodeElement.Kind.child;
                if (comptime calc.is_separate) {
                    const keys = @popCount(old_mask & calc.mask_key);
                    const value_index = keys_index + keys;

                    if (index >= keys_index) kind = NodeElement.Kind.key;
                    if (index >= value_index) kind = NodeElement.Kind.value;
                } else if (index >= keys_index) {
                    kind = NodeElement.Kind.pair;
                }

                it.current += 1;
                return .{
                    .kind = kind,
                    .element = nsv.slice()[index],
                };
            }
        };
        fn node_element_iter(allocator: Allocator, node: Handle) NodeElementIterator {
            return .{ .allocator = allocator, .node = node };
        }

        fn print_ref(allocator: Allocator, ref: Ref) void {
            print_ref_indent(allocator, ref, 1);
            std.debug.print("\n", .{});
        }

        fn repeat(allocator: std.mem.Allocator, s: []const u8, times: u16) ![]u8 {
            const l = s.len;
            const len = l * times;
            const repeated = try allocator.alloc(u8, len);
            for (repeated, 0..) |*dest, i| {
                dest.* = s[i % l];
            }
            return repeated;
        }

        fn print_ref_indent(allocator: Allocator, ref: Ref, indent: u16) void {
            var buffer: [1000]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buffer);
            const alloc = fba.allocator();

            const max = 5;
            const reverse = if (indent < max) max - indent else 0;
            const reverse_in = repeat(alloc, "    ", reverse) catch "    ";

            const node = handle(ref);
            const node_sv = deref(allocator, node);
            std.debug.print("node: {d} {s} tree: {d}", .{ node_sv.count(), reverse_in, handleCount(node, allocator) });
            fba.reset();

            const in = repeat(alloc, "    ", indent) catch "    ";

            var iter = node_element_iter(allocator, node);
            while (iter.next()) |ne| {
                switch (ne.kind) {
                    .child => {
                        const child_ref = ne.element.getChild();
                        std.debug.print("\n{s}child (", .{in});
                        print_ref_indent(allocator, child_ref, indent + 1);
                        std.debug.print("\n{s})", .{in});
                    },
                    .key => {
                        std.debug.print("\n{s}key {any}", .{ in, ne.element.getKey() });
                    },
                    .value => {
                        std.debug.print("\n{s}value {any}", .{ in, ne.element.getValue() });
                    },
                    .pair => {
                        std.debug.print("\n{s}pair ({any}, {any})", .{ in, ne.element.getKey(), ne.element.getValue() });
                    },
                }
            }
        }

        /// Retrieve the value stored with a specific key.
        pub fn get(self: *Self, allocator: Allocator, key: K) ?V {
            if (@sizeOf(Context) != 0)
                @compileError("Cannot infer context " ++ @typeName(Context) ++ ", call getContext instead.");
            return self.getContext(allocator, key, undefined);
        }
        pub fn getContext(self: *Self, allocator: Allocator, key: K, ctx: Context) ?V {
            var ref = self.ref;

            var iter = ctx.prefix(Chunk, key);
            iterSetup(&iter);
            while (iter.next()) |chunk| {
                const current = handle(ref);
                const node = current.deref(allocator);
                const old_mask = node.head.mask;

                const child_mask = calc.maskFromChunk(.child, chunk);
                if (used(old_mask, child_mask)) { // follow path
                    ref = node.bitwiseRef(child_mask).getChild();
                    continue;
                }

                const key_mask = calc.maskFromChunk(.key, chunk);
                if (used(old_mask, key_mask)) { // compare key
                    const old_key = node.bitwiseRef(key_mask).getKey();
                    if (ctx.eql(old_key, key)) {
                        const value_mask = calc.maskFromChunk(.value, chunk);
                        return node.bitwiseRef(value_mask).getValue();
                    }
                }
                return null;
            }
            return null;
        }

        fn print(self: *Self, allocator: Allocator) void {
            if (@sizeOf(Context) != 0)
                @compileError("Cannot infer context " ++ @typeName(Context) ++ ", call printContext instead.");
            self.printContext(allocator, {});
        }
        fn printContext(self: *Self, allocator: Allocator, ctx: Context) void {
            _ = ctx;
            std.debug.print("HAMTUnmanaged{{\n", .{});
            std.debug.print(".ref = ", .{});
            print_ref(allocator, self.ref);
            std.debug.print("}}\n", .{});
        }
    };
}

pub fn HAMT(comptime K: type, comptime V: type, comptime options: Options) type {
    return struct {
        pub const Unmanaged = HAMTUnmanaged(K, V, options);
        const config = options.config(K, V);
        const keep_history = config.keep_history;
        const Context = config.context;
        const Allocator = config.allocator;

        const Self = @This();

        pub fn printDiagnostics() void {
            Unmanaged.printDiagnostics();
        }

        unmanaged: Unmanaged,
        allocator: Allocator,
        ctx: Context,

        /// Create a managed HAMT with an empty context.
        /// If the context is not zero-sized, you must use
        /// initContext(allocator, ctx) instead.
        pub fn init(allocator: Allocator) Self {
            if (@sizeOf(Context) != 0) {
                @compileError("Context must be specified! Call initContext(allocator, ctx) instead.");
            }
            return .{
                .unmanaged = .{},
                .allocator = allocator,
                .ctx = undefined, // ctx is zero-sized so this is safe.
            };
        }

        /// Create a managed HAMT with a context
        pub fn initContext(allocator: Allocator, ctx: Context) Self {
            return .{
                .unmanaged = .{},
                .allocator = allocator,
                .ctx = ctx,
            };
        }

        /// Release the backing HAMTUnmanaged and invalidate it.
        /// This does *not* deinit keys, values, or the context!
        /// If your keys or values need to be released, ensure
        /// that that is done before calling this function.
        pub fn deinit(self: *Self) void {
            self.unmanaged.deinit(self.allocator);
            // self.* = undefined;
        }

        pub fn empty(self: *Self) bool {
            return self.unmanaged.empty(self.allocator);
        }
        pub fn count(self: *Self) usize {
            return self.unmanaged.count(self.allocator);
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            try self.unmanaged.putContext(self.allocator, key, value, self.ctx);
        }
        pub fn remove(self: *Self, key: K) !void {
            try self.unmanaged.removeContext(self.allocator, key, self.ctx);
        }
        pub fn get(self: *Self, key: K) ?V {
            return self.unmanaged.getContext(self.allocator, key, self.ctx);
        }

        pub fn print(self: *Self) void {
            self.unmanaged.printContext(self.allocator, self.ctx);
        }
    };
}

fn hamtTests(comptime K: type, comptime V: type, comptime options: Options, comptime layout: enum { separate, grouped }) !void {
    const t = std.testing;

    const opt = options.withCustomAllocator(K, V, poolAllocator);
    const config = opt.config(K, V);
    const calc = Calculations(K, V, config);

    const H = HAMT(K, V, opt);
    // H.printDiagnostics();
    try t.expect(calc.is_separate == switch (layout) {
        .separate => true,
        .grouped => false,
    });

    const allocator = t.allocator;
    const Pool = opt.customAllocator();
    var pool = Pool.init(allocator);
    defer pool.deinit();

    var h = H.init(if (calc.is_indexed) &pool else allocator);
    defer h.deinit();

    try t.expectEqual(0, h.count());

    try h.put(10, 100);
    try t.expectEqual(1, h.count());

    try h.put(11, 101);
    try h.put(12, 102);
    try t.expectEqual(3, h.count());

    try t.expectEqual(100, h.get(10).?);
    try t.expectEqual(101, h.get(11).?);
    try t.expectEqual(102, h.get(12).?);

    try h.remove(11);
    try t.expectEqual(2, h.count());

    try h.put(7, 100);
    try t.expectEqual(3, h.count());
    try h.put(29, 101);
    try t.expectEqual(4, h.count());
    try h.put(34, 102);
    try t.expectEqual(5, h.count());
    try h.put(35, 103);
    try t.expectEqual(6, h.count());
    try h.put(36, 104);
    try t.expectEqual(7, h.count());
    try h.put(37, 105);
    try t.expectEqual(8, h.count());

    // try calc.calculateSizes(allocator);
}

test HAMT {
    try hamtTests(u64, u64, .{ .min_required_counter = void }, .separate);
    try hamtTests(u64, u64, .{ .min_required_counter = u16 }, .separate);
    try hamtTests(u64, u64, .{}, .separate);
    try hamtTests(u57, u7, .{}, .grouped);
    try hamtTests(u32, u32, .{ .ref = .index }, .separate);
    try hamtTests(u32, u32, .{}, .separate);

    // TODO test different options, test optionals
}

//! Types around the idea of having a `Pool`
//! that supports variable length elements of specific sizes.
const std = @import("std");

pub const PoolOptions = struct {
    pub const SlabCapacity = enum {
        /// Allocates the whole slab at once never re-allocating.
        Static,

        /// Re-allocates partial slabs to grow them dynamically on demand.
        /// This is useful if slabs are big and you are only using a few elements.
        Dynamic,
    };

    slab_capacity: SlabCapacity = .Static,
    amount_choice: type = AmountChoice.Direct,
};

/// A namespace for different strategies to convert between a choice and a specific size.
pub const AmountChoice = struct {
    /// maps the size directly to the choice
    pub const Direct = struct {
        pub const Amount = u6;

        pub fn amountToChoice(amount: Amount) Choice {
            return amount;
        }
        pub fn choiceToAmount(choice: Choice) Amount {
            return choice;
        }
    };

    /// maps the size to a power of 2 that is big enough
    pub const PowersOf2 = struct {
        pub const Amount = u63;

        pub fn amountToChoice(amount: Amount) Choice {
            return std.math.log2_int_ceil(Amount, amount);
        }
        pub fn choiceToAmount(choice: Choice) Amount {
            const amount: Amount = 0b1;
            return amount << choice;
        }
    };
};

pub const Slab = u16;
pub const InvalidSlab: Slab = std.math.maxInt(Slab);
pub const Choice = u6;
pub const choices = std.math.maxInt(Choice) + 1;
pub const Index = u10;
pub const MaxIndex = std.math.maxInt(Index);

/// SlabIndex is used to describe a memory location that fits in a u32,
/// but also allows us to use many different sizes at the same time.
/// It saves memory by using an index instead of a 64 bit pointer,
/// but also gives us flexibilty compares to just using a u32 directly.
/// This allows for example to store the nodes of a HAMT efficiently.
/// Using an index has the additional benefit of getting data structures
/// which are position independent.
pub const SlabIndex = packed struct {
    /// slab is an index for a chunk of memory
    slab: Slab = InvalidSlab,
    /// choice is converted to an amount
    choice: Choice = 0,
    /// index denotes one part of the ´slab´ of ´amount´ elements
    index: Index = 0,

    pub fn zeroRef(self: SlabIndex) bool {
        return self.slab == InvalidSlab;
    }

    pub fn format(self: anytype, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        if (self.slab == InvalidSlab) {
            try writer.print("Invalid", .{});
        } else {
            try writer.print("{}:{}:{}", .{ self.slab, self.choice, self.index });
        }
    }
};

/// A pool and custom memory allocator,
/// that supports space efficient allocation of variable length slices of specific sizes.
/// All allocated sizes are multiples of the base `ElementSize`.
/// Memory gets allocated in size specific slabs that hold multiple parts in one contigous chunk.
/// Slabs can be allocated in whole or grown dynamically, by default they are allocated in one step instead of grown on demand.
pub fn Pool(comptime ElementSize: comptime_int, comptime options: PoolOptions) type {
    return struct {
        const Self = @This();

        const Amount = options.amount_choice.Amount;
        const reallocate = options.slab_capacity == .Dynamic;

        fn getAlignment() usize {
            if (@popCount(@as(usize, ElementSize)) == 1) return ElementSize;
            return std.math.log2_int(usize, ElementSize);
        }
        const alignment = getAlignment();
        const ElementOrFree = [ElementSize]u8;

        comptime {
            if (ElementSize < @sizeOf(SlabIndex)) {
                @compileError(std.fmt.comptimePrint("ElementSize is expected to be >= {d}, but got {d}", .{ @sizeOf(SlabIndex), ElementSize }));
            }
        }

        const SlabSize = 1024;
        const SlabSlice = []align(alignment) ElementOrFree;

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .allocator = allocator };
        }
        pub fn deinit(self: *Self) void {
            for (self.slabs.items) |s| {
                self.allocator.free(s);
            }
            self.slabs.deinit(self.allocator);
        }

        const Capacity = u11;
        const PartialFormat = struct {
            pub fn format(self: anytype, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
                if (self.slab == InvalidSlab) {
                    try writer.print("Unused", .{});
                } else {
                    if (comptime reallocate) {
                        try writer.print("Partial{{ .slab = {}, .next = {}, .capacity = {} }}", .{ self.slab, self.next, self.capacity });
                    } else {
                        try writer.print("Partial{{ .slab = {}, .next = {} }}", .{ self.slab, self.next });
                    }
                }
            }
        };
        const Partial = if (reallocate) struct {
            slab: Slab = InvalidSlab,
            next: Index = 0,
            capacity: Capacity = 0,
            const StartCapacity = 8;
            pub const format = PartialFormat.format;
        } else struct {
            slab: Slab = InvalidSlab,
            next: Index = 0,
            comptime capacity: Capacity = SlabSize,
            const StartCapacity = SlabSize;
            pub const format = PartialFormat.format;
        };

        allocator: std.mem.Allocator,
        slabs: std.ArrayListUnmanaged(SlabSlice) = .{},
        partial: [choices]Partial = [1]Partial{.{}} ** choices,
        freelists: [choices]SlabIndex = [1]SlabIndex{.{}} ** choices,
        fn freelistPtr(self: *Self, si: SlabIndex) *SlabIndex {
            std.debug.assert(si.slab != InvalidSlab);
            const amount: usize = options.amount_choice.choiceToAmount(si.choice);
            const index: usize = si.index;
            return @ptrCast(&self.slabs.items[si.slab][amount * index]);
        }

        pub fn get(self: *Self, si: SlabIndex) ?[]align(alignment) [ElementSize]u8 {
            if (si.slab == InvalidSlab) return null;
            return self.lookupElement(si, [ElementSize]u8);
        }
        pub fn lookup(self: *Self, si: SlabIndex) []align(alignment) [ElementSize]u8 {
            return self.lookupElement(si, [ElementSize]u8);
        }

        pub fn getElement(self: *Self, si: SlabIndex, comptime Element: type) ?[]align(alignment) Element {
            if (si.slab == InvalidSlab) return null;
            return self.lookupElement(si, Element);
        }
        pub fn lookupElement(self: *Self, si: SlabIndex, comptime Element: type) []align(alignment) Element {
            comptime std.debug.assert(@sizeOf(Element) == ElementSize);
            std.debug.assert(si.slab != InvalidSlab);
            const amount: usize = options.amount_choice.choiceToAmount(si.choice);
            const index: usize = si.index;
            return @as([*]align(alignment) Element, @ptrCast(self.slabs.items[si.slab]))[amount * index ..][0..amount];
        }

        pub fn alloc(self: *Self, comptime T: type, amount: Amount) !SlabIndex {
            if (comptime @sizeOf(T) > ElementSize) {
                const fmt =
                    \\this custom allocator only supports types with a size of {d} or smaller
                    \\   • but tried to use it with a type:
                    \\     {s}
                    \\   • which has a size of {d} bytes
                ;
                @compileError(std.fmt.comptimePrint(fmt, .{
                    ElementSize,
                    @typeName(T),
                    @sizeOf(T),
                }));
            }
            const choice = options.amount_choice.amountToChoice(amount);

            const head: *SlabIndex = &self.freelists[choice];
            if (head.slab != InvalidSlab) { // get element from freelist
                const free_element = head.*;
                head.* = self.freelistPtr(head.*).*;
                return free_element;
            }

            const partial = &self.partial[choice];
            if (partial.slab == InvalidSlab) { // ensure capacity
                const slab_len: usize = @as(usize, amount) * Partial.StartCapacity;
                const slab = try self.allocator.alignedAlloc(ElementOrFree, alignment, slab_len);
                errdefer self.allocator.free(slab);

                const slab_index: Slab = @intCast(self.slabs.items.len);
                try self.slabs.append(self.allocator, slab);
                partial.* = .{ .slab = slab_index };
            }

            std.debug.assert(partial.slab != InvalidSlab);

            const slab = partial.slab;
            const index = partial.next;
            if (comptime reallocate) {
                const current: Capacity = @intCast(index);
                const needed_capacity: Capacity = current + 1;
                try self.ensureCapacity(choice, needed_capacity);
            }

            if (index == MaxIndex) {
                partial.* = .{};
            } else {
                partial.next += 1;
            }
            return SlabIndex{
                .slab = slab,
                .choice = choice,
                .index = index,
            };
        }

        pub fn free(self: *Self, si: SlabIndex) void {
            const head: *SlabIndex = &self.freelists[si.choice];
            self.freelistPtr(si).* = head.*;
            head.* = si;
        }

        fn ensureCapacity(self: *Self, choice: Choice, needed_capacity: Capacity) !void {
            const partial = &self.partial[choice];
            if (partial.capacity >= needed_capacity) return;

            const bigger_capacity = growCapacity(partial.capacity, needed_capacity);
            const new_capacity = if (bigger_capacity > SlabSize) SlabSize else bigger_capacity;

            const amount = options.amount_choice.choiceToAmount(choice);
            const slab_len: usize = @as(usize, amount) * @as(usize, new_capacity);

            // this code is similar to ArrayList.ensureTotalCapacityPrecise
            const old_slab = self.slabs.items[partial.slab];
            if (self.allocator.resize(old_slab, new_capacity)) {
                self.slabs.items[partial.slab].len = slab_len;
            } else {
                const new_slab = try self.allocator.alloc(ElementOrFree, slab_len);
                @memcpy(new_slab[0..old_slab.len], old_slab);
                self.allocator.free(old_slab);
                self.slabs.items[partial.slab] = new_slab;
            }
            partial.capacity = new_capacity;
        }

        pub fn format(self: anytype, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
            try writer.print("specificsizes.Pool({},.{{.slab_capacity = .{s}, .amount_choice = {}}})", .{ ElementSize, @tagName(options.slab_capacity), options.amount_choice });
            try writer.print("{{\n", .{});
            try writer.print("    .allocator = {}\n", .{self.allocator});
            try writer.print("    .slabs.items.len = {d}\n", .{self.slabs.items.len});
            try writer.print("    .partial = .{{", .{});
            var empty = true;
            for (self.partial, 0..) |p, choice| {
                if (p.slab != InvalidSlab) {
                    const amount = options.amount_choice.choiceToAmount(@intCast(choice));
                    try writer.print("\n        choice: {d: >2} amount: {d}  {}", .{ choice, amount, p });
                    empty = false;
                }
            }
            if (!empty) try writer.print("\n    ", .{});
            try writer.print("}}\n", .{});

            empty = true;
            try writer.print("    .freelists = .{{", .{});
            for (self.freelists, 0..) |slab_index, choice| {
                if (slab_index.slab != InvalidSlab) {
                    const amount = options.amount_choice.choiceToAmount(@intCast(choice));
                    try writer.print("\n        choice: {d: >2} amount: {d}  {}", .{ choice, amount, slab_index });
                    empty = false;
                }
            }
            if (!empty) try writer.print("\n    ", .{});
            try writer.print("}}\n", .{});

            try writer.print("}}", .{});
        }
    };
}

/// Called when memory growth is necessary. Returns a capacity larger than
/// minimum that grows super-linearly.
/// copied from ArrayList
fn growCapacity(current: usize, minimum: usize) usize {
    var new = current;
    while (true) {
        new +|= new / 2 + 8;
        if (new >= minimum)
            return new;
    }
}

test Pool {
    const t = std.testing;

    const P = Pool(@sizeOf(u32), .{});
    var pool = P.init(t.allocator);
    defer pool.deinit();

    const index = try pool.alloc(u32, 5);
    const slice = pool.lookupElement(index, u32);
    for (slice, [_]u32{ 0, 1, 2, 3, 4 }) |*dest, data| dest.* = data;

    try t.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 4 }, slice);

    const S = struct {
        a: u8,
        b: u8 = 0,
        c: u8 = 0,
        d: u8 = 0,
    };

    const index2 = try pool.alloc(S, 2);
    const slice2 = pool.lookupElement(index2, S);

    slice2[0] = .{ .a = 255 };
    slice2[1] = .{ .a = 35 };

    try t.expectEqualSlices(S, &.{ .{ .a = 255 }, .{ .a = 35 } }, slice2);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const P = Pool(@sizeOf(u32), .{});
    var pool = P.init(allocator);
    defer pool.deinit();

    std.debug.print("pool {}\n", .{pool});

    const index = try pool.alloc(u32, 5);
    std.debug.print("index {}\n", .{index});

    // const index_2 = try pool.alloc(u32, 7);
    // std.debug.print("index_2 {}\n", .{index_2});

    const slice = pool.lookupElement(index, u32);
    for (slice, [_]u32{ 0, 1, 2, 3, 4 }) |*dest, data| dest.* = data;
    std.debug.print("slice {any}\n", .{slice});

    std.debug.print("pool {}\n", .{pool});

    // for (0..100) |i| {
    // const a = i * 10;
    // std.debug.print("a: {}    grow: {}\n", .{ a, growCapacity(0, a) });
    // }
}

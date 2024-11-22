const std = @import("std");
const hamt = @import("dynamic.zig");

fn val(comptime V: type, v: comptime_int) switch (@typeInfo(V)) {
    .Void => void,
    .Int => V,
    .Enum => V,
    .Pointer => blk: {
        if (V == []const u8) break :blk V;
        @compileError("not implemented: " ++ @typeName(V));
    },
    else => |case| @compileError("not implemented: " ++ @tagName(case)),
} {
    return switch (@typeInfo(V)) {
        .Void => {},
        .Int => v,
        .Enum => |e| @enumFromInt(v % e.fields.len),
        .Pointer => blk: {
            if (V == []const u8) break :blk std.fmt.comptimePrint("{d}", .{v});
            @compileError("not implemented: " ++ @typeName(V));
        },
        else => @compileError("not implemented"),
    };
}

fn example(allocator: std.mem.Allocator, comptime K: type, comptime V: type, comptime options: hamt.Options) !void {
    const opt = options.withCustomAllocator(K, V, hamt.poolAllocator);
    const config = opt.config(K, V);
    const calc = hamt.Calculations(K, V, config);

    const Pool = opt.customAllocator();
    var pool = Pool.init(allocator);
    defer pool.deinit();

    const H = hamt.HAMT(K, V, opt);
    var h = H.init(if (calc.is_indexed) &pool else allocator);
    defer h.deinit();

    std.debug.print("\n===================\n", .{});
    H.printDiagnostics();
    h.print();

    try putPrint(&h, val(K, 10), val(V, 120));
    try putPrint(&h, val(K, 11), val(V, 121));
    try putPrint(&h, val(K, 12), val(V, 122));

    try h.remove(val(K, 11));
    try putPrint(&h, val(K, 7), val(V, 123));

    try putPrint(&h, val(K, 29), val(V, 101));
    try putPrint(&h, val(K, 34), val(V, 102));
    try putPrint(&h, val(K, 35), val(V, 103));
    try putPrint(&h, val(K, 36), val(V, 104));
    try putPrint(&h, val(K, 37), val(V, 105));
}

fn exampleBig(gpa: anytype, allocator: std.mem.Allocator, comptime K: type, comptime V: type, comptime options: hamt.Options) !void {
    const memory_before = gpa.total_requested_bytes;

    const opt = options.withCustomAllocator(K, V, hamt.poolAllocator);
    const config = opt.config(K, V);
    const calc = hamt.Calculations(K, V, config);

    const Pool = opt.customAllocator();
    var pool = Pool.init(allocator);
    defer pool.deinit();

    const H = hamt.HAMT(K, V, opt);
    var h = H.init(if (calc.is_indexed) &pool else allocator);
    defer h.deinit();

    std.debug.print("\n===================\n", .{});
    H.printDiagnostics();
    h.print();

    // const seed = 1718955009594;
    const seed: u64 = @intCast(std.time.milliTimestamp());
    std.debug.print("seed: {d}\n", .{seed});

    var default_rng = std.rand.DefaultPrng.init(seed);
    const rng = default_rng.random();

    const T = K;

    for (0..10000) |_| {
        const k = rng.uintLessThan(T, std.math.maxInt(T));
        const v = rng.uintLessThan(T, std.math.maxInt(T));
        try h.put(k, v);
    }

    const memory_after = gpa.total_requested_bytes;
    const memory_delta = memory_after - memory_before;
    std.debug.print("using {d} bytes of memory\n", .{memory_delta});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{
        .enable_memory_limit = true,
    }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try example(allocator, u64, u128, .{});
    try example(allocator, u64, u64, .{});
    try example(allocator, u40, u13, .{});
    try example(allocator, u57, u7, .{});
    try example(allocator, u56, u8, .{});
    try example(allocator, u8, u8, .{});

    try example(allocator, u128, void, .{});
    try example(allocator, u64, void, .{});
    try example(allocator, u64, void, .{ .min_required_counter = void });
    try example(allocator, u8, void, .{});

    const ABCD = enum(u2) { A, B, C, D };
    try example(allocator, ABCD, u64, .{});
    try example(allocator, ABCD, u62, .{});
    try example(allocator, ABCD, ABCD, .{});
    try example(allocator, u62, ABCD, .{});

    try exampleBig(&gpa, allocator, u64, u64, .{});
    try exampleBig(&gpa, allocator, u32, u32, .{});
    try exampleBig(&gpa, allocator, u8, u8, .{});

    try example(allocator, u64, u64, .{ .hashing = false });
    try example(allocator, u32, u32, .{ .hashing = false });
    try example(allocator, []const u8, u32, .{});
}

fn putPrint(h: anytype, key: anytype, value: anytype) !void {
    std.debug.print("\n-------------------\n", .{});
    try h.put(key, value);
    h.print();
}

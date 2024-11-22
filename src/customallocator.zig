//! Allocator needs to be either:
//!    * a std.mem.Allocator
//!    * a custom Allocator
//!      - a custom allocator is passed as a direct pointer
//!
//!      - it has a lookup function:
//!        ```
//!        pub fn lookup(self:*CustomAlloc, ref:Ref) []Element
//!        ```
//!
//!      - it has an alloc and free function:
//!        ```
//!        pub fn alloc(self: *CustomAlloc, comptime T: type, amount: Amount) !Ref
//!        pub fn free(self: *Self, ref: Ref) void
//!        ```
//!
//!      - Ref can be a specific index type that is
//!        adapted to how the custom allocator works
const std = @import("std");

const expected_allocator_msg =
    \\Allocator needs to be either:
    \\   • a std.mem.Allocator
    \\   • a custom Allocator
    \\     - a custom allocator is passed as a direct pointer
    \\
    \\     - it has a lookup function:
    \\       pub fn lookup(self:*CustomAlloc, ref:Ref) []Element
    \\
    \\     - it has an alloc and free function:
    \\       pub fn alloc(self: *CustomAlloc, comptime T: type, amount: Amount) !Ref
    \\       pub fn free(self: *Self, ref: Ref) void
    \\
    \\     - Ref can be a specific index type that is
    \\       adapted to how the custom allocator works
;

/// Checks whether the given allocator type is a supported allocator,
/// otherwise creates a compile error.
pub fn checkAllocator(comptime Allocator: type) void {
    if (Allocator == std.mem.Allocator) return;
    if (!isRefAllocator(Allocator)) @compileError(expected_allocator_msg);
}

/// Returns true if the given type is a valid ref allocator.
pub fn isRefAllocator(comptime AllocatorOrPtr: type) bool {
    return switch (AllocatorOrPtr) {
        std.mem.Allocator => false,
        else => blk: {
            const Allocator = std.meta.Child(AllocatorOrPtr);
            const f1 = @hasDecl(Allocator, "lookup");
            const f2 = @hasDecl(Allocator, "alloc");
            const f3 = @hasDecl(Allocator, "free");
            const ok = f1 and f2 and f3;
            break :blk ok;
        },
    };
}

/// Returns the reference type that is being used by a custom allocator.
pub fn LookupFunctionGetReference(comptime AllocatorPtr: type) type {
    const Allocator = std.meta.Child(AllocatorPtr);
    const lookupFn = @TypeOf(Allocator.lookup);
    switch (@typeInfo(lookupFn)) {
        .Fn => |f| {
            const Index = f.params[1].type.?;
            return Index;
        },
        else => @compileError("LookupFunctionGetReference: not supported"),
    }
}

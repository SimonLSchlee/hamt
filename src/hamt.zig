//! This library is a work-in-progress/preview.
//!
//! This library provides HAMT and StencilArray implementations.
//!
//! It also explores the idea of index based custom allocators,
//! that effectively behave like memory pools for instances that
//! are accessed via an index/reference. Those allow the user to
//! use indices instead of pointers to save memory and gain
//! position independence for the data structure.
const std = @import("std");

pub const context = @import("context.zig");
pub const customallocator = @import("customallocator.zig");
pub const specificsizes = @import("specificsizes.zig");

pub const static = @import("static.zig");
pub const dynamic = @import("dynamic.zig");

test {
    std.testing.refAllDeclsRecursive(@This());
}

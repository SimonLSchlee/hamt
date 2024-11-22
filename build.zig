const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root = b.path("src/hamt.zig");

    // module
    _ = b.addModule("hamt", .{
        .root_source_file = root,
        .target = target,
        .optimize = optimize,
    });

    // docs
    const doc_lib = b.addTest(.{
        .name = "hamt",
        .root_source_file = root,
        .target = target,
        .optimize = .Debug,
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = doc_lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });

    // This is a workaround to prevent the build system from ignoring changes and just
    // claiming that it can cache the docs output.
    // For some reason the build system doesn't see that the inputs to the docs
    // generation have changed and that the docs need to be regenerated.
    // So with this we force it to rebuild, by running the tests and having the tests
    // detect that things have changed and because `install_docs` depends on that test run,
    // it gets refreshed too.
    const run_docs = b.addRunArtifact(doc_lib);
    install_docs.step.dependOn(&run_docs.step);

    const docs_step = b.step("docs", "Generate documentation");
    docs_step.dependOn(&install_docs.step);
    b.default_step.dependOn(docs_step);

    // test
    const test_filters = b.option([]const []const u8, "test-filter", "Skip tests that do not match any filter") orelse &[0][]const u8{};
    const tests = b.addTest(.{
        .root_source_file = root,
        .target = target,
        .optimize = optimize,
        .filters = test_filters,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}

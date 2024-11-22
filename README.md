
# HAMT and StencilArrays

A work-in-progress implementation of HAMTs (Hash Array Mapped Tries)
and StencilArrays (StencilVectors) in Zig.

Based on the ideas presented in theses papers:

- [Ideal Hash Trees (Bagwell, Phil  2001)](https://infoscience.epfl.ch/record/64398)
- [Runtime and Compiler Support for HAMTs](https://www-old.cs.utah.edu/plt/publications/dls21-tzf.pdf)

## Description

HAMTs are bitwise tries that have a compact memory layout, they are useful
data structures that can be used as a key-value mapping, they also can be
used as a persistent data structure, which means that old versions keep
working and different versions can share nodes, this can be useful to
implement things like undo, or wherever it is helpful to keep old versions
of a value, without having to create full copies manually.

This implementation contains dynamic variants for runtime usage with both
pointer and index support, imperative and persistent (old versions stay valid)
behavior based on user configuration.

It also has simplified static implementations that can be modified at comptime
without needing an allocator and then frozen and queried at runtime.

If you have ideas for improvements, feel free to open an issue.

## Docs
https://simonlschlee.github.io/hamt/

## Examples

Take a look at `src/example.zig` for some example code, you can run this after
cloning this repository by running `zig run src/example.zig`, the documentation
also contains usage examples in the form of doc-tests.

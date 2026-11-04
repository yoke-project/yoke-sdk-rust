# @YOKE_ID@

A Yoke Plugin written in Rust.

| Path | What it holds |
| --- | --- |
| `yoke-plugin.yaml` | the file every step of `yoke-compose` reads: the identity, the build, the architectures |
| `plugin/` | the Plugin's source, and its Manifest, `@YOKE_ID@.yaml`, kept beside it |
| `try/` | an application project that uses the Plugin, to see it work |

Build it with `yoke-compose build`; publish it by tagging this repository `vX.Y.Z`.

Building for another architecture than this machine's needs its target, `rustup target add
aarch64-unknown-linux-gnu`, and a linker for it; or a `build.container` in `yoke-plugin.yaml` that has both.

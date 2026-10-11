# yoke-sdk-rust

## What it is

The Rust family of Yoke's SDKs: the library a Rust program uses to run as a Plugin unit in a Yoke
instance, over a base it shares with the libraries to come, and the harness that holds it to the
contract under the conformance suite. It is published as one crate, `yoke-sdk`, with the modules
`yoke_sdk::base` and `yoke_sdk::plugin`.

It is not the Core, and it holds no contract definitions of its own: it takes them from the
`yoke-proto` crate.

## What is where

| Path | What it holds |
| --- | --- |
| `src/base.rs` | What the libraries share: the connection, the envelope, a refusal as an error |
| `src/plugin.rs` | The library a Plugin unit is written with |
| `harness/` | The library's harness for the conformance suite, a crate of the workspace that is never published |
| `tests/` | The libraries' tests, each beside its description (`.std.md`) |
| `checks/`, `ci/` | This repository's own checks, and the scripts continuous integration runs |

## Working on it

Rust, through `cargo`, and [`just`](https://just.systems) 1.58.0 or newer. `just develop` also puts
the published verification tool, `yoke-verify`, on the `PATH`, which `just test` needs. The
`Containerfile` builds a container with the toolchain.

| Command | What it does |
| --- | --- |
| `just build` | Builds the workspace, the harness included, from the lockfile |
| `just test` | Runs the checks and every test, and checks each description against its tests |
| `just lint` | Parses every shell script |
| `just fmt` | Fails, naming each file, when a Rust file is not formatted |
| `just develop` | Verifies the installed `just` against a floor, given as `just develop 1.58.0`, and installs `yoke-verify` |
| `just release` | Publishes the crate at the tag that names this commit; run by the release, not by hand |

## Where the depth is

The documentation at [docs.yoke-project.dev](https://docs.yoke-project.dev/): the
[reference](https://docs.yoke-project.dev/reference/) for the contract the library speaks, and the
[contributors' section](https://docs.yoke-project.dev/contributing/) for how the work is done. The
crate's documentation is on [docs.rs](https://docs.rs/yoke-sdk).

## Licence

Apache 2.0, in [`LICENSE`](LICENSE), with a [`NOTICE`](NOTICE).

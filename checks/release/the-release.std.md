# The release

| | |
| --- | --- |
| **Feature** | this family publishes one crate, `yoke-sdk`, carrying the base and the plugin library as its modules, at the version the tree states — which is the version the library says it is — and it publishes it with `yoke`'s published release verb, at its release tag, by trusted publishing; the harness is never published |
| **Planning item** | yoke-project/yoke-sdk-rust#26 |

## yoke-sdk-rust:the-release.01 — one crate, yoke-sdk, carries the base and the plugin library, the licence and the notice; the harness is not published

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/80 §The layout · arch/90-sdks/01 §One project, and what the base may hold · prj_structure/95 §The licences |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | list what the crate `yoke-sdk` would carry, and read the workspace's manifests |
| **Expected** | it carries its manifest, `LICENSE`, `NOTICE`, and the sources of `yoke_sdk::base` and `yoke_sdk::plugin`, and nothing of the harness, the checks or the scripts; the harness's crate says it is not published, and no other crate of the workspace is published |

## yoke-sdk-rust:the-release.02 — the crate is packaged at the tree's version and no other, and the library says that version

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/85 §What a release is · prj_structure/85 §The release record · arch/90-sdks/01 §What each library declares |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | package the crate at the version the tree states; package it at another; read the line the library says it is |
| **Expected** | the first writes `yoke-sdk-<version>.crate`; the second is refused, naming both versions, and writes nothing; the line is `yoke-sdk-rust` followed by the crate's version, so a tag, the crate and the line cannot disagree |

## yoke-sdk-rust:the-release.03 — the release verb publishes the crate with yoke's published verb, and the release run offers trusted publishing

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command · prj_structure/97 §The definitions, and the four families that are not Go |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the `release` verb, and the workflow `release.yml` |
| **Action** | read them |
| **Expected** | the verb runs `yoke`'s published release verb, asked for the crate `yoke-sdk`; the workflow runs at a `v` tag, may request an identity token, exchanges it for a crates.io credential in a step that does not fail the run when the exchange is refused, gives the verb that credential, and keeps the verb's output as the artifact `manifest-lines` — no credential is stored anywhere |

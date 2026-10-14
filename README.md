# elri

One graph, your own models, any view. A local-first environment for knowledge,
actions, and interfaces.

## Layout

```
protocols/   protobuf source of truth; codegen for Rust and TypeScript
crates/
  elri-proto   generated messages and ws-proto bindings
  elri-core    the core library: graph, storage, sync, authorization, scripts
  elri-server  authoritative synchronization server
  elri-cli     `elri` command-line client for the local core
apps/elri      Tauri shell (desktop + Android) and the React renderer
packages/proto generated TypeScript contracts
docs/adrs      architecture
```

## Toolchain

- Rust 1.98 (`rust-toolchain.toml`), `just`, `cargo-nextest`, `cargo-deny`
- `easyp`, `protoc-gen-prost`, `protoc-gen-prost-crate`,
  `protoc-gen-ws-rs` (`cargo install --git https://github.com/gopherex/ws-proto protoc-gen-ws-rs`)
- Node 22, pnpm 11; `@gopherex/*` packages come from GitHub Packages
  (`.npmrc` expects a token with `read:packages` in `~/.npmrc`)
- Tauri on Linux: `webkit2gtk4.1-devel gtk3-devel libsoup3-devel javascriptcoregtk4.1-devel`

## Commands

```
just proto    # regenerate Rust + TypeScript from protocols/
just check    # fmt, clippy, cargo-deny
just test     # nextest
just web      # typecheck + biome
just dev      # Tauri dev: core in-process, renderer on 127.0.0.1:1430
```

Outside Tauri, point the renderer at a running core with
`VITE_ELRI_CORE_URL` and `VITE_ELRI_CORE_TOKEN` from `endpoint.json` in the
core's data directory.

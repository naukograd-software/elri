set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

# Regenerate Rust and TypeScript protobuf code from protocols/.
proto:
    cd protocols && easyp -cfg easyp.rs.yaml lint
    rm -rf crates/elri-proto/src/*
    cd protocols && easyp -cfg easyp.rs.yaml generate
    rm -rf packages/proto/src/*
    cd protocols && PATH="$PWD/../packages/proto/node_modules/.bin:$PATH" easyp -cfg easyp.ts.yaml generate
    cargo fmt -p elri-proto

# Rust validation gate.
check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo deny check

test:
    cargo nextest run --workspace

# Frontend validation gate.
web:
    pnpm -r typecheck
    pnpm exec biome check .

build:
    cargo build --workspace

# Local core + renderer in dev mode. GDK_BACKEND=x11 sidesteps WebKitGTK's
# Wayland protocol errors; drop it if your session renders fine without.
dev:
    GDK_BACKEND=x11 pnpm --filter @elri/app tauri dev

# Cross-build the core for Android (needs cargo-ndk and an NDK).
android:
    cargo ndk -t arm64-v8a build -p elri-core

# Build the Android application (debug APK). Needs JAVA_HOME, ANDROID_HOME,
# NDK_HOME, cargo-ndk, and the aarch64-linux-android target.
android-app:
    cd apps/elri && pnpm exec tauri android build --debug --target aarch64 --apk

all: proto check test web

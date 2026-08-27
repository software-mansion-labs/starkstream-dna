# syntax=docker/dockerfile:1.7

# Build stage: compile starkstream-dna-starknet from source.
# Keep the tag in step with rust-toolchain.toml, or every build re-downloads the
# toolchain. Base images are tags, not digests, so a commit does not build
# reproducibly; what the release contract rests on is release.yml building a
# commit at most once and reusing the published git-<sha> image.
FROM rust:1.94-bookworm AS builder

ENV CARGO_TERM_COLOR=always \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        llvm-dev \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY . .

# Cache mounts help local rebuilds only: they are not exported to the registry
# or the Actions cache, and the COPY above invalidates this layer every commit,
# so CI compiles from scratch. Fixing that needs cargo-chef or equivalent.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p starkstream-dna-starknet \
 && cp /src/target/release/starkstream-dna-starknet /usr/local/bin/starkstream-dna-starknet

# Runtime stage: slim debian with TLS roots.
FROM debian:bookworm-slim AS runtime

# groupadd is explicit because `useradd --user-group` clamps a system group to
# SYS_GID_MAX (999 here), not 65532.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 dna \
    && useradd --system --uid 65532 --gid 65532 --no-create-home dna \
    && install -d -o 65532 -g 65532 /data

COPY --from=builder /usr/local/bin/starkstream-dna-starknet /usr/local/bin/starkstream-dna-starknet

USER 65532:65532
WORKDIR /data

# The foyer disk cache lives here. Unset, it defaults to $HOME/.local/share/dna,
# and uid 65532 has no home, so an unconfigured container fails on startup.
ENV DNA_CACHE_DIR=/data

EXPOSE 7007

ENTRYPOINT ["/usr/local/bin/starkstream-dna-starknet"]
CMD ["start"]

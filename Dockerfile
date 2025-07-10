FROM rust:1.88 AS base
# Curl google to quickly debug if networking is broken inside docker before proceeding with the next steps
RUN curl https://google.com
RUN cargo install cargo-binstall
RUN cargo binstall sccache --version ^0.7
RUN cargo binstall cargo-chef --version ^0.1
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache


FROM base AS planner
WORKDIR /app
COPY src/ .
COPY Cargo.toml .
COPY Cargo.lock .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
  cargo chef prepare --recipe-path recipe.json

FROM base as builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
  cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
  cargo build --release
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/fugu /usr/local/bin
EXPOSE 3301
ENTRYPOINT ["/usr/local/bin/fugu"]

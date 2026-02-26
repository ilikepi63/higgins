FROM rust:1.93.0-slim AS build

RUN rustup target add x86_64-unknown-linux-musl

RUN apt update && \
    apt install -y musl-tools musl-dev  protobuf-compiler clang && \
    update-ca-certificates

WORKDIR /
COPY . ./

RUN cargo build --release --package  higgins --target x86_64-unknown-linux-musl

# RUN adduser \
#     --disabled-password \
#     --gecos "" \
#     --home "/nonexistent" \
#     --shell "/sbin/nologin" \
#     --no-create-home \
#     --uid "10001" \
#     "higgins_user"

FROM scratch

# USER higgins_user:higgins_user

COPY --from=build target/x86_64-unknown-linux-musl/release/higgins /bin/higgins

# USER higgins:higgins

WORKDIR /

ENTRYPOINT ["/bin/higgins"]

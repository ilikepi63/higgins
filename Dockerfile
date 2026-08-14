FROM rust:1.93.0-slim AS build

RUN rustup target add x86_64-unknown-linux-musl

RUN apt update && \
    apt install -y musl-tools musl-dev protobuf-compiler clang && \
    update-ca-certificates

WORKDIR /
COPY . ./

RUN cargo build --release --package higgins --target x86_64-unknown-linux-musl

RUN useradd \
    --system \
    --no-create-home \
    --home-dir "/nonexistent" \
    --shell "/sbin/nologin" \
    --user-group \
    --uid 10001 \
    higgins_user

FROM scratch

COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

COPY --from=build target/x86_64-unknown-linux-musl/release/higgins /bin/higgins

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

USER higgins_user:higgins_user

WORKDIR /

EXPOSE 4932

ENTRYPOINT ["/bin/higgins"]
CMD ["--port=4932", "--dir=/data"]

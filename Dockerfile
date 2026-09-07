FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
LABEL org.opencontainers.image.source="https://github.com/srcfl/ftwdb"
COPY --from=build /src/target/release/ftw /src/target/release/ftwdb-shadow /src/target/release/ftwdb-shadow-reconcile /usr/local/bin/
COPY LICENSE /usr/share/doc/ftwdb/LICENSE
RUN install -d -o 100 -g 101 -m 0700 /var/lib/ftwdb-shadow /run/ftwdb-shadow
USER 100:101
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/ftwdb-shadow"]
CMD ["/var/lib/ftwdb-shadow", "/run/ftwdb-shadow/shadow.sock"]

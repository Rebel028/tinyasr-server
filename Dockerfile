# tinyasr-server: a Flux-compatible streaming STT websocket. Two images:
#   --target slim   no models inside; at start it fetches the configured ones into /models (mount a volume there)
#   --target full   (default) every tinyasr model inside; starts the server directly, needs no network
# OCI labels (source, revision, version, licenses) are set by the CI build, not here.
FROM rust:1-bookworm AS build
ARG RUSTFLAGS=""
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# the start-up self-test clip, embedded into the binary
COPY tests/data/ru5.s16 ./tests/data/
RUN RUSTFLAGS="${RUSTFLAGS}" cargo build --release --locked

FROM debian:bookworm-slim AS slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
 && install -d -o nobody -g nogroup /models
COPY --from=build /src/target/release/tinyasr-server /usr/local/bin/tinyasr-server
# the fetch script reads the pinned tinyasr rev from Cargo.toml
COPY Cargo.toml /opt/tinyasr/
COPY scripts /opt/tinyasr/scripts
ENV TINYASR_MODEL=/models/en-ru-uk.tzf TINYASR_VAD=/models/silero_vad.onnx TINYASR_LANGS=ru TINYASR_BEAM=4 PORT=8000
EXPOSE 8000
USER nobody
HEALTHCHECK --interval=10s --timeout=3s --start-period=120s CMD curl -fsS -o /dev/null http://localhost:8000/health || exit 1
ENTRYPOINT ["sh", "/opt/tinyasr/scripts/entrypoint-slim.sh"]
CMD ["tinyasr-server"]

FROM slim AS full
RUN sh /opt/tinyasr/scripts/fetch-models.sh /models all
ENTRYPOINT []
CMD ["tinyasr-server"]

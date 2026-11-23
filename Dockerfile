# tinyasr-server: a Flux-compatible streaming STT websocket with the en-ru-uk model and Silero VAD baked in.
# OCI labels (source, revision, version, licenses) are set by the CI build, not here.
FROM rust:1-bookworm AS build
ARG RUSTFLAGS=""
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY scripts/fetch-models.sh scripts/
RUN sh scripts/fetch-models.sh /models
COPY src ./src
RUN RUSTFLAGS="${RUSTFLAGS}" cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /models /models
COPY --from=build /src/target/release/tinyasr-server /usr/local/bin/tinyasr-server
ENV TINYASR_MODEL=/models/en-ru-uk.tzf TINYASR_VAD=/models/silero_vad.onnx TINYASR_LANGS=ru TINYASR_BEAM=4 PORT=8000
EXPOSE 8000
USER nobody
HEALTHCHECK --interval=10s --timeout=3s CMD curl -fsS -o /dev/null http://localhost:8000/health || exit 1
CMD ["tinyasr-server"]

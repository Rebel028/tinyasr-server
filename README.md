# tinyasr-server

A streaming speech-to-text server built on [tinyasr](https://github.com/dkaraush/tinyasr). It runs on the CPU and has no GPU path.
It speaks a compatible subset of the Deepgram Flux streaming protocol (`GET /v2/listen`). A Flux client switches to it by changing only its base URL.
It targets live phone calls: 16 kHz PCM16 mono, in 40 ms frames, with one websocket per call and tens of calls at the same time. The default language is Russian.

The server has no async runtime. Each connection gets one socket thread and one session worker thread. All sessions share one loaded model.
The socket thread reads the audio into a bounded queue and writes the messages. The worker feeds the queue into the library, so decode spikes (80 to 250 ms) never block the socket.

## Protocol subset

### Upgrade

```
GET /v2/listen?model=flux-general-multi&encoding=linear16&sample_rate=16000&language_hint=ru&eot_threshold=0.7&eot_timeout_ms=5000
Authorization: Token <key>
```

| Query key | Handling |
|---|---|
| `encoding` | Required. Must be `linear16` (PCM16 little-endian, mono). Any other value gets a 400. |
| `sample_rate` | Must be `16000`, or left out. Any other value gets a 400. |
| `language_hint` | Optional and repeatable. A comma list also works. `ru`, `ru-RU` and `ru_RU` all map to `ru`. A language the model does not have gets a 400. With no hint the server uses `TINYASR_LANGS`. Each extra language makes every re-decode cost one more pass. |
| `eot_threshold` | Default 0.7, range 0 to 1. |
| `eot_timeout_ms` | Default 5000, range 100 to 60000. |
| `model`, `keyterm`, `numerals`, anything else | Accepted and ignored. |

Refusals are a plain HTTP status with a JSON body such as `{"err_code":"INVALID_AUTH","err_msg":"Invalid credentials."}`:

| Status | When |
|---|---|
| 401 | `TINYASR_TOKEN` is set and the header is not `Token <value>` (`Bearer <value>` is also accepted). |
| 400 | A query value is bad. |
| 426 | The request has no websocket upgrade. |
| 503 | `TINYASR_MAX_SESSIONS` sessions are already running. |

The 101 response has a `dg-request-id` header.

### Client to server

- Binary frames: PCM16 LE audio of any length. An odd trailing byte waits for the next frame.
- `{"type":"CloseStream"}`: the server finishes the session and sends what is left (the last `EndOfTurn`). Then it closes the socket with code 1000.
- Ping frames get a pong. Any other text message is ignored.

### Server to client

At connect:

```json
{"type":"Connected","request_id":"0f04dd14-2024-4e1f-9d43-39be22b73f2b","sequence_id":0}
```

Then `TurnInfo` messages (values illustrative):

```json
{"type":"TurnInfo","request_id":"…","sequence_id":12,"event":"StartOfTurn","turn_index":0,"audio_window_start":0.32,"audio_window_end":1.28,"transcript":"сервер","words":[],"end_of_turn_confidence":0.000}
{"type":"TurnInfo","request_id":"…","sequence_id":13,"event":"Update","turn_index":0,"audio_window_start":0.32,"audio_window_end":1.28,"transcript":"сервер","words":[],"end_of_turn_confidence":0.000}
{"type":"TurnInfo","request_id":"…","sequence_id":30,"event":"EndOfTurn","turn_index":0,"audio_window_start":0.32,"audio_window_end":4.36,"transcript":"Сервер базы данных упал после обновления платежей.","words":[],"end_of_turn_confidence":0.712,"trigger":"model"}
{"type":"TurnInfo","request_id":"…","sequence_id":31,"event":"Update","turn_index":1,"audio_window_start":4.36,"audio_window_end":4.52,"transcript":"","words":[],"end_of_turn_confidence":0.000}
```

- `StartOfTurn`: the first recognized word of a turn.
- `Update`: the transcript of the open turn changed. Committed sentences are followed by the words that are not final yet.
- `EndOfTurn`: the final transcript of the turn. `turn_index` goes up by one per turn.
- Heartbeat: when no other message went out for 200 ms, the server repeats the current state as an `Update`. This also happens in silence, where the transcript is empty and `turn_index` is the next turn. Clients can use the steady stream as a liveness signal.
- If a session fails inside (a panic), the server sends `{"type":"Error","sequence_id":n,"code":"INTERNAL_SERVER_ERROR","description":"…"}` and closes with 1011. Other sessions keep running.

End of turn:

1. Model: the library's end-of-turn head says `p >= eot_threshold` at a pause, and the library then closes that sentence. The trigger is `model`.
2. Timeout: no new word for `eot_timeout_ms` of audio after the last word of the turn. The trigger is `timeout`.
3. `CloseStream` with a turn still open. The trigger is `manual`.

All times are seconds of the client's audio stream, dropped audio included.

## Known differences from the vendor protocol

- `words` is always `[]`. The library has no per-word confidence. Its word times are not sent.
- `end_of_turn_confidence` is not a value updated on every message. It is the probability from the library's end-of-turn head, and the head runs only at pauses (0.24 s and 0.48 s into the silence). The value is 0 during speech and until the first pause. An `EndOfTurn` caused by a timeout or a close carries the last value of the head, often below the threshold.
- The end of turn comes about 0.8 s after the end of speech (measured on Russian TTS). That is the head at 0.24 s plus the library's sentence freeze at 0.48 s plus the decode. With `eot_threshold` under 0.5, the library still freezes the sentence only at p > 0.5. A turn that the head scores between the threshold and 0.5 therefore ends after 3 s of silence.
- The timeout counts audio, not wall time. If a client stops sending audio, the timeout never fires.
- The numeric fields are JSON numbers. The vendor's schema documents `audio_window_*` and `end_of_turn_confidence` as strings, but the reference client this server was checked against parses them as numbers.
- `EagerEndOfTurn`, `TurnResumed`, `eager_eot_threshold`, `Configure`/`ConfigureSuccess`, `ForceEndTurn`, `Warning`, `keyterm`, `numerals`, `redact` and `languages` are not implemented. Unknown query keys and text messages are ignored, not refused.
- Only `linear16` at 16 kHz is supported. There are no containers, no mu-law and no other rates.
- `model` is ignored. The served model is `TINYASR_MODEL` (en, ru, uk).
- The 503 for a full server and the JSON error bodies are this server's own. The vendor's limits and bodies differ.
- When the queue overflows (the decode falls behind by more than `TINYASR_QUEUE_MS`), the oldest audio is dropped. The amount is logged. The transcript then misses that audio, and `audio_window_*` keep the client's timeline.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8000` | Listen port (all interfaces). |
| `TINYASR_MODEL` | `/models/en-ru-uk.tzf` | Recognizer file. |
| `TINYASR_VAD` | `/models/silero_vad.onnx` | Silero VAD v5. |
| `TINYASR_LANGS` | `ru` | Languages used when the client sends no `language_hint`. |
| `TINYASR_BEAM` | `4` | Beam of the re-decode. `1` is greedy, which is cheaper and can lose punctuation. |
| `TINYASR_TOKEN` | unset | If set, clients must send `Authorization: Token <value>`. |
| `TINYASR_MAX_SESSIONS` | CPU count | Concurrent sessions. The next upgrade gets a 503. |
| `TINYASR_QUEUE_MS` | `3000` | Audio queue per session. When it overflows, the oldest audio is dropped. |

The log goes to stderr. There is one line per session start (peer, request id, languages, thresholds) and one per session end (reason, duration, audio seconds, dropped seconds, turns). Transcripts are never logged.

## Build and run

The host needs only Docker:

```bash
docker build -t tinyasr-server .
docker run --rm -p 8000:8000 -e TINYASR_TOKEN=change-me tinyasr-server
# or a released image (linux/amd64 and linux/arm64): ghcr.io/<owner>/tinyasr-server:<version>
# client base URL: ws://<host>:8000/v2/listen
```

`--build-arg RUSTFLAGS="-C target-cpu=native"` gives 7 to 10 percent, but the image then runs only on CPUs like the build host.

Tests (unit tests plus integration tests over real sockets) need the models in `models/`:

```bash
sh scripts/fetch-models.sh   # pinned tinyasr rev, sha256 checked
docker run --rm -v tasr-cargo:/usr/local/cargo/registry -v tasr-cargo-git:/usr/local/cargo/git -v "$PWD:/src" -w /src \
  rust:1-bookworm cargo test --release --locked -- --nocapture
```

`-e FLUX_CONCURRENT=16` sets the number of sessions in the concurrency test (the default is 4).

Measured on an i9-10900KF (10 cores, WSL2) with beam 4 and the Russian 5 s clip at real-time pace plus 4 s of silence:

| Sessions | End of turn after end of speech, mean / max | Max gap between messages | Dropped audio |
|---|---|---|---|
| 1 | 0.78 to 0.87 s | 216 ms | 0 |
| 4 | 0.75 / 0.80 s | 216 ms | 0 |
| 8 | 0.76 / 0.80 s | 220 ms | 0 |
| 16 | 1.05 / 1.34 s | 220 ms | 0 |
| 24 | 2.33 / 2.95 s | 240 ms | 0 |

## Releases

The version lives in `Cargo.toml` and the server logs it at start. To release, change it and push to the default branch. When the tests pass and `ghcr.io/<owner>/<repo>:<version>` does not exist yet, CI builds the image for linux/amd64 and linux/arm64, pushes it as `<version>` and `latest`, and tags the commit `v<version>`. A push that does not change the version releases nothing. The image labels (source, revision, version) come from the GitHub context.
The model files and the library are pinned to one tinyasr rev, the one in `Cargo.toml`. `scripts/fetch-models.sh` reads it from there and checks the files by sha256, so a new rev with changed models fails until the sums are updated.

## License

GPL-3.0-or-later (see `LICENSE`), the same as tinyasr. The server links tinyasr, so it is GPL as well. A client talks to it over the network, and that does not bring the client under the GPL. If you ship the image, ship this source with it.

# tinyasr-server

A streaming speech-to-text server built on [tinyasr](https://github.com/dkaraush/tinyasr). It runs on the CPU and has no GPU path.
It speaks a compatible subset of the Deepgram Flux streaming protocol (`GET /v2/listen`). A Flux client switches to it by changing only its base URL.
It targets live phone calls: 16 kHz PCM16 mono, in 40 ms frames, with one websocket per call and tens of calls at the same time. The default language is Russian.

The server has no async runtime. Each connection gets one socket thread and one session worker thread. All sessions share one loaded model.
The socket thread reads the audio into a bounded queue and writes the messages. The worker feeds the queue into the library, so decode spikes (80 to 250 ms) never block the socket.

## Quickstart

Run the server (the image has every model inside and needs no network):

```bash
docker run --rm -p 8000:8000 -e TINYASR_TOKEN=change-me ghcr.io/rebel028/tinyasr-server
curl http://localhost:8000/health   # {"status":"ok"}
```

Point a Flux client at it by changing only the base URL and the key:

```
ws://localhost:8000/v2/listen?model=flux-general-multi&encoding=linear16&sample_rate=16000&language_hint=ru
Authorization: Token change-me
```

Send PCM16 mono audio at 16 kHz as binary frames, and read `TurnInfo` messages back. The sections below give the details.

Common changes:

| Want | Do |
|---|---|
| Another language set | `-e TINYASR_MODEL=/models/fr-de.tzf -e TINYASR_LANGS=fr` (models: `ar`, `en-es-it-pt`, `en-fa`, `en-km`, `en-ru-uk`, `fr-de`, `id-ms`, `uz`) |
| The spoken language of each turn, also outside the model's languages | `-e TINYASR_LID=/models/tinylid.bin` |
| A small image that fetches only the models it uses | the `slim` tag with `-v tinyasr-models:/models` (see [Build and run](#build-and-run)) |
| No auth (local tests only) | leave `TINYASR_TOKEN` out |

Do not expose the port to a public network directly. Put a proxy with TLS and connection limits in front.

## tinyasr-console - Test Web UI
A [browser console](https://github.com/Rebel028/tinyasr-console) for tinyasr-server. It streams microphone or file audio to /v2/listen and shows the turns along with the raw event log of the websocket.

<img heigt="720" alt="image" src="https://github.com/user-attachments/assets/1cb83fcc-e15c-4ffb-8020-db89859f2fd8" />

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
| `eager_eot_threshold` | Optional, range 0.3 to 0.9, not greater than `eot_threshold` (both get a 400). Turns on `EagerEndOfTurn` and `TurnResumed`. Left out, the server sends neither. |
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
{"type":"TurnInfo","request_id":"…","sequence_id":12,"event":"StartOfTurn","turn_index":0,"audio_window_start":0.32,"audio_window_end":1.28,"transcript":"сервер","words":[],"languages":["ru"],"languages_hinted":["ru"],"end_of_turn_confidence":0.000,"lag_ms":40,"dropped_ms":0}
{"type":"TurnInfo","request_id":"…","sequence_id":13,"event":"Update","turn_index":0,"audio_window_start":0.32,"audio_window_end":1.28,"transcript":"сервер","words":[],"languages":["ru"],"languages_hinted":["ru"],"end_of_turn_confidence":0.000,"lag_ms":40,"dropped_ms":0}
{"type":"TurnInfo","request_id":"…","sequence_id":30,"event":"EndOfTurn","turn_index":0,"audio_window_start":0.32,"audio_window_end":4.36,"transcript":"Сервер базы данных упал после обновления платежей.","words":[],"languages":["ru"],"languages_hinted":["ru"],"end_of_turn_confidence":0.712,"trigger":"model","silence_ms":480,"lag_ms":120,"dropped_ms":0}
{"type":"TurnInfo","request_id":"…","sequence_id":31,"event":"Update","turn_index":1,"audio_window_start":4.36,"audio_window_end":4.52,"transcript":"","words":[],"languages":[],"languages_hinted":["ru"],"end_of_turn_confidence":0.000,"lag_ms":0,"dropped_ms":0}
```

- `StartOfTurn`: the first recognized word of a turn.
- `Update`: the transcript of the open turn changed. Committed sentences are followed by the words that are not final yet.
- `EndOfTurn`: the final transcript of the turn. `turn_index` goes up by one per turn.
- `EagerEndOfTurn` (only with `eager_eot_threshold`): a probable end of turn. The transcript is the current text of the turn, never empty, and `end_of_turn_confidence` is the head's score. See [Eager end of turn](#eager-end-of-turn).
- `TurnResumed` (only after an `EagerEndOfTurn`): the transcript changed after the `EagerEndOfTurn`. It carries the new text and takes the place of that `Update`. The turn goes on.
- `languages`: the language of the turn, one code. It is the language the recognizer is decoding in, or with `TINYASR_LID` on, the language tinylid hears in the `EndOfTurn`. It is `[]` while the transcript is empty.
- `languages_hinted`: the `language_hint` codes of the session, as the server maps them (`ru-RU` becomes `ru`). It is `[]` when the client sent no hint and the server uses `TINYASR_LANGS`.
- Heartbeat: when the recognizer has processed 0.2 s of audio since the last message of any kind, the server repeats the current state as an `Update`. This also happens in silence and in noise, where the transcript is empty and `turn_index` is the next turn. Flux says: "Update messages are sent for approximately every 0.25 seconds of transcribed audio, regardless of transcript updates". This server uses 0.2 s because the audio comes in 40 ms frames, and 0.25 s would round up to 0.28 s. The clock is processed audio, not wall time. The wall-clock spacing is 0.2 s plus the decode time, measured at 0.3 to 0.5 s during speech. If the recognizer stalls, the messages stop, and a client watchdog can detect that. No audio in means no messages out.
- `lag_ms` (every `TurnInfo`, this server's own): audio received minus audio processed when the message went out. It grows when the recognizer falls behind.
- `dropped_ms` (every `TurnInfo`, this server's own): audio the queue has dropped in this session so far (see `TINYASR_QUEUE_MS`).
- `silence_ms` (`EndOfTurn` only, this server's own): the silence since the pause began, when the library reported the pause. It is absent when the model has no end-of-turn head or the turn ended during speech.
- If a session fails inside (a panic), the server sends `{"type":"Error","sequence_id":n,"code":"INTERNAL_SERVER_ERROR","description":"…"}` and closes with 1011. Other sessions keep running.

End of turn:

1. Model: the library's end-of-turn head says `p >= eot_threshold` at a pause, and the library then closes that sentence. The trigger is `model`.
2. Timeout: `eot_timeout_ms` of silence, as in Flux (the longest silence before a forced end of turn; new speech resets the timer). The silence starts where the library reports a pause began (its end-of-turn head runs at 0.24 s into it), and a word newer than the pause clears it. Speech that resumes without a recognized word does not clear it until the next pause replaces it. With a model that has no end-of-turn head, the timeout counts from the end of the newest word instead, and a session counts as such until its first pause. The trigger is `timeout`.
3. `CloseStream` with a turn still open. The trigger is `manual`.

All times are seconds of the client's audio stream, dropped audio included.

### Eager end of turn

With `eager_eot_threshold` set, the server sends `EagerEndOfTurn` when the head scores an open turn with a nonempty transcript at or above that threshold and no earlier `EagerEndOfTurn` of the turn is still outstanding. The head runs at 0.24 s of silence, so the eager event comes at about 0.24 s of silence and `EndOfTurn` at about 0.48 s.

- Any change of the transcript after an `EagerEndOfTurn` sends `TurnResumed` with the new text instead of an `Update`. A later pause can then give a new `EagerEndOfTurn`.
- Flux says: "The transcript in EagerEndOfTurn will exactly match the transcript in the subsequent EndOfTurn event". When the turn ends and its final text differs from the outstanding `EagerEndOfTurn`, the server sends `TurnResumed`, `EagerEndOfTurn` (both with the final text, the eager score) and `EndOfTurn` back to back. This happens when the library's final re-decode before the `Commit` changes words or punctuation.
- The timeout and `CloseStream` paths send only `EndOfTurn`. They never send an `EagerEndOfTurn` of their own, except for the sequence above. Flux does not say what its timeout does.
- `lag_ms` and `dropped_ms` are on these messages too. `trigger` and `silence_ms` are on `EndOfTurn` only.

Where the library limits this: its head always reports a score at 0.24 s of silence, but its own decision threshold is fixed at 0.5. A yes (p > 0.5) leads to the `Commit` at 0.48 s. After a no, the head runs once more at 0.48 s of the same pause. A no also makes the library append `...` to the shown text, so with an `eager_eot_threshold` below 0.5 an eager event on a no is followed at once by a `TurnResumed` with that text. The server does not hide it. Values below 0.5 therefore give eager events that the model path mostly does not confirm. When speech resumes between 0.24 s and 0.48 s, the library sends no event, and the new words show up as a `TurnResumed`.

Alternative design, not implemented: the library could finish the sentence at 0.24 s, so that `EndOfTurn` itself comes 0.24 s earlier. That needs a change of the upstream library or a fork. Its own comment says the freeze waits for 0.48 s so that the re-decode hears the end of the last word, so the accuracy would have to be measured again.

## Known differences from the vendor protocol

- `words` is always `[]`. The library has no per-word confidence. Its word times are not sent.
- `end_of_turn_confidence` is not a value updated on every message. It is the probability from the library's end-of-turn head, and the head runs only at pauses (0.24 s and 0.48 s into the silence). The value is 0 during speech and until the first pause. An `EndOfTurn` caused by a timeout or a close carries the last value of the head, often below the threshold.
- The end of turn comes about 0.8 s after the end of speech (measured on Russian TTS). That is the head at 0.24 s plus the library's sentence freeze at 0.48 s plus the decode. With `eot_threshold` under 0.5, the library still freezes the sentence only at p > 0.5. A turn that the head scores between the threshold and 0.5 therefore ends after 3 s of silence.
- The timeout counts audio, not wall time. If a client stops sending audio, the timeout never fires.
- The numeric fields are JSON numbers. The vendor's schema documents `audio_window_*` and `end_of_turn_confidence` as strings, but the reference client this server was checked against parses them as numbers.
- `Configure`/`ConfigureSuccess`, `ForceEndTurn`, `Warning`, `keyterm`, `numerals`, and `redact` are not implemented. Unknown query keys and text messages are ignored, not refused.
- `languages` holds one code per turn, never several. Without `TINYASR_LID` it is the recognizer's pick among the session's languages. With `TINYASR_LID`, the `EndOfTurn` carries tinylid's top language from the first 3 s of the turn. That can be a language the model cannot transcribe, so a client that hinted `en` and gets `ru` knows the speaker is outside its hint. `StartOfTurn` and `Update` keep the recognizer's pick. Short turns (a word or two) give unreliable labels.
- Only `linear16` at 16 kHz is supported. There are no containers, no mu-law and no other rates.
- `model` is ignored. The served model is `TINYASR_MODEL` (by default en, ru, uk).
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
| `TINYASR_LID` | unset | Path of `tinylid.bin`, normally `/models/tinylid.bin` (inside the full image; the slim image fetches it). If set, `languages` of each `EndOfTurn` comes from tinylid, which adds about 90 ms to the end of turn. Unset, the model is not loaded. |
| `TINYASR_QUEUE_MS` | `3000` | Audio queue per session. When it overflows, the oldest audio is dropped. |
| `TINYASR_TRACE` | unset | `1`: one `trace` log line per second per session: `t_s`, `push_n`, `push_ms`, `push_max_ms` (time in the library this second), `proc_s`, `recv_s`, `queue_s`, `dropped_s` (audio processed, received, queued, dropped), `open_words`, `open_s` (the open sentence), `turn_chars`, `msgs`, `msg_bytes` (sent this second), `rss_mb`. |

The log goes to stderr. There is one line per session start (peer, request id, languages, thresholds) and one per session end (reason, duration, audio seconds, dropped seconds, turns). Transcripts are never logged.

## Build and run

The host needs only Docker:

```bash
docker build -t tinyasr-server .
docker run --rm -p 8000:8000 -e TINYASR_TOKEN=change-me tinyasr-server
# client base URL: ws://<host>:8000/v2/listen
```

There are two images, both for linux/amd64 and linux/arm64:

| Image | Tags | Models | Start |
|---|---|---|---|
| full (the default build target) | `<version>`, `latest` | Every tinyasr model, the VAD and tinylid are inside, in `/models` (about 300 MB). | Runs the server directly. It needs no network. |
| slim (`docker build --target slim`) | `<version>-slim`, `slim` | None inside. | First fetches the files that `TINYASR_MODEL`, `TINYASR_VAD` and `TINYASR_LID` name into `/models`, then runs the server. |

The slim image fetches from the tinyasr repository at the rev this server was built against, and checks each file by sha256. A file that is already in `/models` with the right checksum is not fetched again. Mount a volume there, and only the first start needs the network:

```bash
docker run --rm -p 8000:8000 -v tinyasr-models:/models ghcr.io/<owner>/tinyasr-server:slim
```

To serve another model, set `TINYASR_MODEL` and `TINYASR_LANGS` together, for example `-e TINYASR_MODEL=/models/fr-de.tzf -e TINYASR_LANGS=fr`. This works in both images. The models are `ar`, `en-es-it-pt`, `en-fa`, `en-km`, `en-ru-uk`, `fr-de`, `id-ms` and `uz`.
A model file of your own also works in the slim image: put it into the mounted `/models` under a name that is not in that list. The image uses it without a checksum check.

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

Capacity on a small machine: measured with a 2-core limit on real-time long takes, the decoder lag starts to grow above 2 parallel sessions (above 1 session on 1 core), with beam 4 and with beam 1. Beam 1 (`TINYASR_BEAM=1`) roughly halves the time to the first partial, at an accuracy cost: it is cheaper and can lose punctuation, and the tinyasr README gives Russian FLEURS WER 5.47 % for the beam 4 second pass against 7.22 % for the greedy streaming pass. Accuracy at beam 1 was not measured here.

## Releases

The version lives in `Cargo.toml` and the server logs it at start. To release, change it and push to the default branch. When the tests pass and `ghcr.io/<owner>/<repo>:<version>` does not exist yet, CI builds the image for linux/amd64 and linux/arm64, pushes it as `<version>` and `latest`, and tags the commit `v<version>`. A push that does not change the version releases nothing. The image labels (source, revision, version) come from the GitHub context.
The model files and the library are pinned to one tinyasr rev, the one in `Cargo.toml`. `scripts/fetch-models.sh` reads it from there and checks the files by sha256, so a new rev with changed models fails until the sums are updated.
Twice a month the `upstream` workflow compares that rev with upstream. When it is behind, the workflow opens a pull request with the new rev, lock file and sums, and starts the tests on it. It does not merge and does not release.

## License

GPL-3.0-or-later (see `LICENSE`), the same as tinyasr. The server links tinyasr, so it is GPL as well. A client talks to it over the network, and that does not bring the client under the GPL. If you ship the image, ship this source with it.

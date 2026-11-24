//! Integration tests: start the server binary, talk the Flux subset to it over real sockets.
//! Needs models/en-ru-uk.tzf, models/silero_vad.onnx and models/tinylid.bin (see README) and tests/data/ru5.s16.
//! FLUX_CONCURRENT=<n> sets the number of sessions in `concurrent_sessions` (default 4).

use serde_json::Value;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Error, HandshakeError, Message, WebSocket};

const REF: &str = "сервер базы данных упал после обновления платежей";
const Q: &str = "model=flux-general-multi&encoding=linear16&sample_rate=16000&language_hint=ru&eot_threshold=0.6&eot_timeout_ms=3000&unknown=1";
const FRAME: usize = 1280; // 40 ms of 16 kHz PCM16

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A server that passed its self-test.
fn start(envs: &[(&str, &str)]) -> Server {
    let s = listening(envs);
    assert_eq!(get(s.port, "/health"), (200, "{\"status\":\"ok\"}".into()));
    s
}

/// A server that answers HTTP; it binds only after its self-test.
fn listening(envs: &[(&str, &str)]) -> Server {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_tinyasr-server"))
        .env("PORT", port.to_string())
        .env("TINYASR_MODEL", "models/en-ru-uk.tzf")
        .env("TINYASR_VAD", "models/silero_vad.onnx")
        .envs(envs.iter().copied())
        .spawn()
        .unwrap();
    let s = Server { child, port };
    let t = Instant::now();
    while get(port, "/health").0 == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "server did not start"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    s
}

/// Plain HTTP GET: (status, body); status 0 when the connection fails.
fn get(port: u16, path: &str) -> (u16, String) {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
        return (0, String::new());
    };
    let _ = write!(s, "GET {path} HTTP/1.1\r\nHost: x\r\n\r\n");
    let mut r = String::new();
    let _ = s.read_to_string(&mut r);
    let code = r
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (code, r.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}

/// Websocket upgrade; Err(status) when the server refuses it.
/// `token` goes out as `Token <value>`, or as given when it already names a scheme (`Bearer x`).
fn connect(port: u16, query: &str, token: Option<&str>) -> Result<WebSocket<TcpStream>, u16> {
    let mut req = format!("ws://127.0.0.1:{port}/v2/listen?{query}")
        .into_client_request()
        .unwrap();
    if let Some(t) = token {
        let v = if t.contains(' ') {
            t.to_string()
        } else {
            format!("Token {t}")
        };
        req.headers_mut()
            .insert("Authorization", v.parse().unwrap());
    }
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(2))).unwrap();
    s.set_nodelay(true).unwrap();
    match tungstenite::client(req, s) {
        Ok((ws, _)) => Ok(ws),
        Err(HandshakeError::Failure(Error::Http(r))) => Err(r.status().as_u16()),
        Err(HandshakeError::Interrupted(mut m)) => loop {
            // the 2 ms read timeout can interrupt the handshake
            match m.handshake() {
                Ok((ws, _)) => return Ok(ws),
                Err(HandshakeError::Interrupted(n)) => m = n,
                Err(HandshakeError::Failure(Error::Http(r))) => return Err(r.status().as_u16()),
                Err(e) => panic!("handshake: {e}"),
            }
        },
        Err(e) => panic!("handshake: {e}"),
    }
}

/// What a client saw: text messages with their arrival time, and when the clip's speech ended.
#[derive(Default)]
struct Log {
    msgs: Vec<(Instant, Value)>,
    speech_end: Vec<Instant>,
    closed: bool,
}

impl Log {
    /// Read until `until`; true when the server closed the socket.
    fn read(&mut self, ws: &mut WebSocket<TcpStream>, until: Instant) -> bool {
        loop {
            match ws.read() {
                Ok(Message::Text(t)) => self
                    .msgs
                    .push((Instant::now(), serde_json::from_str(t.as_str()).unwrap())),
                Ok(Message::Close(_)) => self.closed = true,
                Ok(_) => {}
                Err(Error::Io(e))
                    if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(Error::ConnectionClosed) => return true,
                Err(e) => panic!("read: {e}"),
            }
            if Instant::now() >= until {
                return false;
            }
        }
    }

    fn stream(&mut self, ws: &mut WebSocket<TcpStream>, audio: &[u8]) -> Vec<Instant> {
        self.stream_as(ws, audio, FRAME)
    }

    /// Send the audio at real-time pace in 40 ms frames of `frame` bytes, reading all the while; returns when each frame went out.
    fn stream_as(
        &mut self,
        ws: &mut WebSocket<TcpStream>,
        audio: &[u8],
        frame: usize,
    ) -> Vec<Instant> {
        let t0 = Instant::now();
        let mut sent = vec![];
        for (i, f) in audio.chunks(frame).enumerate() {
            self.read(ws, t0 + Duration::from_millis(40 * i as u64));
            ws.send(Message::Binary(f.to_vec().into())).unwrap();
            sent.push(Instant::now());
        }
        sent
    }

    /// The clip, then `silence` s of zeros. The speech end: the last frame over -40 dBFS.
    fn turn(&mut self, ws: &mut WebSocket<TcpStream>, clip: &[u8], silence: f64) {
        let zeros = vec![0u8; (silence * 32000.0) as usize];
        self.turn_as(ws, clip, clip, FRAME, &zeros);
    }

    /// `wire` (the 16 kHz PCM16 `clip` in another format, 40 ms in `frame` bytes), then `silence` in that format.
    fn turn_as(
        &mut self,
        ws: &mut WebSocket<TcpStream>,
        clip: &[u8],
        wire: &[u8],
        frame: usize,
        silence: &[u8],
    ) {
        let sent = self.stream_as(ws, wire, frame);
        let loud = clip.chunks(FRAME).rposition(|f| {
            let (pairs, _) = f.as_chunks::<2>();
            let x: Vec<f64> = pairs
                .iter()
                .map(|b| i16::from_le_bytes(*b) as f64 / 32768.0)
                .collect();
            (x.iter().map(|v| v * v).sum::<f64>() / x.len() as f64).sqrt() > 0.01
        });
        self.speech_end.push(sent[loud.unwrap()]);
        self.stream_as(ws, silence, frame);
    }

    fn close(&mut self, ws: &mut WebSocket<TcpStream>) {
        ws.send(Message::text(r#"{"type":"CloseStream"}"#)).unwrap();
        assert!(
            self.read(ws, Instant::now() + Duration::from_secs(10)),
            "server did not close the socket"
        );
    }

    fn turns(&self, event: &str) -> Vec<(Instant, &Value)> {
        self.msgs
            .iter()
            .filter(|m| m.1["type"] == "TurnInfo" && m.1["event"] == event)
            .map(|m| (m.0, &m.1))
            .collect()
    }

    /// Over the `TurnInfo` messages before `to`: the longest wall-clock gap and the largest step of `audio_window_end`.
    fn gaps(&self, to: usize) -> (Duration, f64) {
        let m = &self.msgs[1..to];
        let wall = m
            .windows(2)
            .map(|w| w[1].0 - w[0].0)
            .max()
            .unwrap_or_default();
        let audio = m
            .windows(2)
            .map(|w| {
                w[1].1["audio_window_end"].as_f64().unwrap()
                    - w[0].1["audio_window_end"].as_f64().unwrap()
            })
            .fold(0.0, f64::max);
        (wall, audio)
    }
}

/// White noise at about -35 dBFS from a fixed generator: audio without speech, the same in every run.
fn noise(secs: f64) -> Vec<u8> {
    let mut x = 1u32;
    (0..(secs * 16000.0) as usize)
        .flat_map(|_| {
            x = x.wrapping_mul(1664525).wrapping_add(1013904223);
            ((x >> 16) as u16 as i16 / 32).to_le_bytes()
        })
        .collect()
}

fn clip() -> Vec<u8> {
    std::fs::read("tests/data/ru5.s16").expect("tests/data/ru5.s16")
}

/// 16 kHz PCM16 resampled by `l / m`: a Hamming-windowed sinc low-pass at 0.9 of the lower Nyquist, no delay.
fn resample(pcm: &[u8], l: usize, m: usize) -> Vec<i16> {
    use std::f64::consts::PI;
    let x: Vec<f64> = pcm
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| i16::from_le_bytes(*b) as f64)
        .collect();
    let (n, r) = (64 * l.max(m) + 1, 16000.0 * l as f64);
    let fc = 0.9 * 16000f64.min(r / m as f64) / 2.0 / r;
    let h: Vec<f64> = (0..n)
        .map(|k| {
            let t = k as f64 - (n / 2) as f64;
            let sinc = if t == 0.0 {
                2.0 * fc
            } else {
                (2.0 * PI * fc * t).sin() / (PI * t)
            };
            l as f64 * sinc * (0.54 - 0.46 * (2.0 * PI * k as f64 / (n - 1) as f64).cos())
        })
        .collect();
    (0..x.len() * l / m)
        .map(|j| {
            let s: f64 = (0..n)
                .filter_map(|k| {
                    let u = (j * m + n / 2).checked_sub(k)?;
                    (u % l == 0).then(|| h[k] * x.get(u / l).copied().unwrap_or(0.0))
                })
                .sum();
            s.round().clamp(-32768.0, 32767.0) as i16
        })
        .collect()
}

/// G.711 mu-law.
fn mulaw(s: i16) -> u8 {
    let x = (s as i32).abs().min(32635) + 0x84;
    let e = 31 - (x as u32).leading_zeros() as i32 - 7;
    !((if s < 0 { 0x80 } else { 0 }) | (e << 4) | ((x >> (e + 3)) & 0x0f)) as u8
}

fn words(s: &str) -> Vec<String> {
    s.to_lowercase()
        .replace('ё', "е")
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(String::from)
        .collect()
}

fn text(v: &Value) -> &str {
    v["transcript"].as_str().unwrap()
}

/// A finished turn with the expected words; returns the time from the end of speech to its EndOfTurn.
fn check_turn(log: &Log, index: u64) -> Duration {
    let starts = log.turns("StartOfTurn");
    assert!(
        starts.iter().any(|m| m.1["turn_index"] == index),
        "no StartOfTurn {index}"
    );
    let eot = log
        .turns("EndOfTurn")
        .into_iter()
        .find(|m| m.1["turn_index"] == index)
        .unwrap_or_else(|| panic!("no EndOfTurn {index}"));
    let got = words(text(eot.1));
    for w in words(REF) {
        assert!(
            got.contains(&w),
            "turn {index}: {w:?} missing in {:?}",
            text(eot.1)
        );
    }
    eot.0 - log.speech_end[index as usize]
}

#[test]
fn turns_heartbeats_and_close() {
    let srv = start(&[("TINYASR_TOKEN", "secret")]);
    let clip = clip();
    let mut ws = connect(srv.port, Q, Some("secret")).unwrap();
    let mut log = Log::default();
    log.turn(&mut ws, &clip, 4.0);
    log.turn(&mut ws, &clip, 4.0);
    let idle_from = log.msgs.len();
    log.stream(&mut ws, &vec![0u8; 32000]);
    log.stream(&mut ws, &noise(1.0));
    let silence_from = log.msgs.len();
    // no audio, no messages (as Flux): 0.5 s for the worker to finish the queued audio, then 1 s without a message
    log.read(&mut ws, Instant::now() + Duration::from_millis(500));
    let quiet_from = log.msgs.len();
    log.read(&mut ws, Instant::now() + Duration::from_secs(1));
    let quiet = log.msgs.len() - quiet_from;
    log.close(&mut ws);

    assert_eq!(log.msgs[0].1["type"], "Connected");
    let lat: Vec<Duration> = (0..2).map(|i| check_turn(&log, i)).collect();
    for (_, m) in log.turns("EndOfTurn") {
        assert_eq!(
            (&m["languages"], &m["languages_hinted"]),
            (&serde_json::json!(["ru"]), &serde_json::json!(["ru"])),
            "{m}"
        );
    }

    // Updates of the first turn carry growing text
    let ups: Vec<usize> = log
        .turns("Update")
        .iter()
        .filter(|m| m.1["turn_index"] == 0)
        .map(|m| words(text(m.1)).len())
        .filter(|&n| n > 0)
        .collect();
    assert!(
        ups.len() >= 3 && ups.last() > ups.first(),
        "updates of turn 0: {ups:?}"
    );

    // the EndOfTurn of turn 0 comes before turn 1 starts; idle Updates in between say turn 1 with no text
    let eot0 = log
        .msgs
        .iter()
        .position(|m| m.1["event"] == "EndOfTurn")
        .unwrap();
    let sot1 = log
        .msgs
        .iter()
        .position(|m| m.1["event"] == "StartOfTurn" && m.1["turn_index"] == 1)
        .unwrap();
    let idle = &log.msgs[eot0 + 1..sot1];
    assert!(
        idle.len() >= 8
            && idle.iter().all(|m| m.1["event"] == "Update"
                && m.1["turn_index"] == 1
                && text(&m.1).is_empty()
                && m.1["languages"] == serde_json::json!([])),
        "idle: {} messages",
        idle.len()
    );
    for w in log.msgs[1..].iter().map(|m| &m.1) {
        assert!(
            w["sequence_id"].is_u64()
                && w["audio_window_end"].is_f64()
                && w["end_of_turn_confidence"].is_f64()
                && w["lag_ms"].is_u64()
                && w["dropped_ms"] == 0,
            "{w}"
        );
    }
    assert!(
        log.msgs
            .windows(2)
            .all(|w| w[1].1["sequence_id"].as_u64() > w[0].1["sequence_id"].as_u64())
    );

    for (_, m) in log.turns("EndOfTurn") {
        assert!(m["silence_ms"].as_u64().is_some_and(|s| s >= 240), "{m}");
    }

    // heartbeats while audio streams, in speech, in silence and in noise: one per 0.2 s of processed audio.
    // After a decode the worker catches up between two polls, so one step can exceed 0.2 s (measured up to 0.6 s).
    let (gap, step) = log.gaps(silence_from);
    assert!(gap <= Duration::from_secs(1), "max gap {gap:?}");
    assert!(step <= 0.8, "max audio step {step:.2} s");
    assert_eq!(quiet, 0, "messages without audio");
    // 1 s of silence and 1 s of noise: the Updates go on, with the same (empty) next turn
    let idle = &log.msgs[idle_from..silence_from];
    assert!(
        idle.len() >= 8
            && idle.iter().all(|m| m.1["event"] == "Update"
                && m.1["turn_index"] == 2
                && text(&m.1).is_empty()),
        "silence and noise: {:?}",
        idle.iter().map(|m| &m.1).collect::<Vec<_>>()
    );
    assert!(log.closed, "no close frame");
    // no eager_eot_threshold: no eager events
    assert!(log.turns("EagerEndOfTurn").is_empty() && log.turns("TurnResumed").is_empty());
    eprintln!(
        "turn latency after speech end: {:?}; max message gap {gap:?}, max audio step {step:.2} s; {} messages",
        lat,
        log.msgs.len()
    );
}

#[test]
fn stalled_worker_goes_quiet() {
    let srv = start(&[("TINYASR_TEST_STALL_MS", "1500")]); // the worker sleeps 1.5 s after 1 s of audio
    let mut ws = connect(srv.port, Q, None).unwrap();
    let mut log = Log::default();
    log.stream(&mut ws, &vec![0u8; 4 * 32000]);
    let (gap, i) = log
        .msgs
        .windows(2)
        .enumerate()
        .map(|(i, w)| (w[1].0 - w[0].0, i + 1))
        .max()
        .unwrap();
    assert!(gap >= Duration::from_millis(1200), "max gap {gap:?}");
    assert!(log.msgs.len() > i + 3, "messages did not resume");
    let lag = log.msgs[i].1["lag_ms"].as_u64().unwrap();
    assert!(lag >= 500, "lag after the stall {lag} ms");
    log.close(&mut ws);
    eprintln!("stall: gap {gap:?}, lag after it {lag} ms");
}

#[test]
fn eager_end_of_turn() {
    let srv = start(&[]);
    let mut ws = connect(srv.port, &format!("{Q}&eager_eot_threshold=0.5"), None).unwrap();
    let mut log = Log::default();
    log.turn(&mut ws, &clip(), 4.0);
    log.close(&mut ws);
    check_turn(&log, 0);
    let eager = log.turns("EagerEndOfTurn");
    let eot = log.turns("EndOfTurn");
    assert!(
        eager.first().is_some_and(|e| e.0 <= eot[0].0),
        "no EagerEndOfTurn before the EndOfTurn"
    );
    // every EndOfTurn that follows an EagerEndOfTurn without a TurnResumed in between has its transcript
    let mut open: Option<&str> = None;
    for m in log.msgs.iter().map(|m| &m.1) {
        match m["event"].as_str() {
            Some("EagerEndOfTurn") => {
                assert!(!text(m).is_empty(), "{m}");
                open = Some(text(m));
            }
            Some("TurnResumed") => open = None,
            Some("EndOfTurn") => {
                if let Some(t) = open.take() {
                    assert_eq!(t, text(m));
                }
            }
            _ => {}
        }
    }
    eprintln!(
        "eager: {:?} before the EndOfTurn {:?} (audio_window_end {} and {}); events {:?}",
        eot[0].0 - eager[0].0,
        text(eot[0].1),
        eager[0].1["audio_window_end"],
        eot[0].1["audio_window_end"],
        log.msgs
            .iter()
            .filter_map(|m| m.1["event"].as_str())
            .filter(|e| *e != "Update")
            .collect::<Vec<_>>()
    );
}

#[test]
fn failed_self_test_gives_503() {
    let srv = listening(&[("TINYASR_TEST_SELFTEST_TEXT", "другой текст")]);
    let (code, body) = get(srv.port, "/health");
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("self-test failed"), "{body}");
}

#[test]
fn auth_params_and_limit() {
    let srv = start(&[("TINYASR_TOKEN", "secret"), ("TINYASR_MAX_SESSIONS", "1")]);
    assert_eq!(get(srv.port, "/health").0, 200);
    assert_eq!(connect(srv.port, Q, Some("wrong")).err(), Some(401));
    assert_eq!(connect(srv.port, Q, None).err(), Some(401));
    assert_eq!(
        connect(srv.port, "encoding=opus&sample_rate=16000", Some("secret")).err(),
        Some(400)
    );
    assert_eq!(
        connect(
            srv.port,
            "encoding=linear16&sample_rate=44100",
            Some("secret")
        )
        .err(),
        Some(400)
    );
    assert_eq!(
        connect(
            srv.port,
            "encoding=linear16&language_hint=de",
            Some("secret")
        )
        .err(),
        Some(400)
    );
    assert_eq!(
        connect(
            srv.port,
            "encoding=linear16&eot_threshold=1.5",
            Some("secret")
        )
        .err(),
        Some(400)
    );
    assert_eq!(
        connect(
            srv.port,
            "encoding=linear16&eager_eot_threshold=0.2",
            Some("secret")
        )
        .err(),
        Some(400)
    );
    // greater than eot_threshold, whatever the key order
    assert_eq!(
        connect(
            srv.port,
            "encoding=linear16&eager_eot_threshold=0.6&eot_threshold=0.5",
            Some("secret")
        )
        .err(),
        Some(400)
    );

    let mut first = connect(srv.port, Q, Some("secret")).unwrap();
    assert_eq!(connect(srv.port, Q, Some("secret")).err(), Some(503));
    // a client that vanishes without a close frees its session promptly
    first
        .send(Message::Binary(vec![0u8; FRAME].into()))
        .unwrap();
    drop(first);
    let t = Instant::now();
    let mut second = loop {
        // the Bearer scheme is accepted too
        match connect(srv.port, Q, Some("Bearer secret")) {
            Ok(ws) => break ws,
            Err(503) if t.elapsed() < Duration::from_secs(2) => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(c) => panic!("status {c} {:?} after the first client left", t.elapsed()),
        }
    };
    eprintln!("slot free {:?} after the client vanished", t.elapsed());
    let mut log = Log::default();
    log.close(&mut second);
    assert!(log.closed);
}

#[test]
fn concurrent_sessions() {
    let n: usize = std::env::var("FLUX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let srv = start(&[("TINYASR_MAX_SESSIONS", "64")]);
    // no websocket upgrade (and no token configured, so the request reaches that check)
    assert_eq!(get(srv.port, "/v2/listen?encoding=linear16").0, 426);
    let clip = clip();
    let port = srv.port;
    let res: Vec<(Duration, (Duration, f64))> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..n)
            .map(|k| {
                let clip = &clip;
                s.spawn(move || {
                    std::thread::sleep(Duration::from_millis(200 * k as u64 % 2000)); // spread the starts
                    let mut ws = connect(port, Q, None).unwrap();
                    let mut log = Log::default();
                    log.turn(&mut ws, clip, 4.0);
                    let to = log.msgs.len();
                    log.close(&mut ws);
                    (check_turn(&log, 0), log.gaps(to))
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let lat_max = res.iter().map(|r| r.0).max().unwrap();
    let lat_mean = res.iter().map(|r| r.0).sum::<Duration>() / n as u32;
    let gap = res.iter().map(|r| r.1.0).max().unwrap();
    let step = res.iter().map(|r| r.1.1).fold(0.0, f64::max);
    eprintln!(
        "concurrent n={n}: end-of-turn after speech end mean {lat_mean:?} max {lat_max:?}; max message gap {gap:?}, max audio step {step:.2} s"
    );
    assert!(gap <= Duration::from_secs(1), "max gap {gap:?}");
    assert!(step <= 0.8, "max audio step {step:.2} s");
}

#[test]
fn input_formats() {
    let srv = start(&[]);
    let clip = clip();
    let pcm = |s: &[i16]| s.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<u8>>();
    let at8 = resample(&clip, 1, 2);
    // encoding, rate, the clip as sent, bytes per 40 ms, 2 s of silence
    let cases: Vec<(&str, usize, Vec<u8>, usize, Vec<u8>)> = vec![
        ("linear16", 16000, clip.clone(), FRAME, vec![0; 64000]),
        ("linear16", 8000, pcm(&at8), 640, vec![0; 32000]),
        (
            "mulaw",
            8000,
            at8.iter().map(|&v| mulaw(v)).collect(),
            320,
            vec![0xff; 16000],
        ),
        (
            "linear16",
            48000,
            pcm(&resample(&clip, 3, 1)),
            3840,
            vec![0; 192000],
        ),
    ];
    let logs: Vec<Log> = std::thread::scope(|s| {
        let hs: Vec<_> = cases
            .iter()
            .map(|(enc, rate, wire, frame, silence)| {
                let clip = &clip;
                s.spawn(move || {
                    let q = format!(
                        "encoding={enc}&sample_rate={rate}&language_hint=ru&eot_threshold=0.6"
                    );
                    let mut ws = connect(srv.port, &q, None).unwrap();
                    let mut log = Log::default();
                    log.turn_as(&mut ws, clip, wire, *frame, silence);
                    log.close(&mut ws);
                    log
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let end = |log: &Log| {
        log.turns("EndOfTurn")[0].1["audio_window_end"]
            .as_f64()
            .unwrap()
    };
    for ((enc, rate, ..), log) in cases.iter().zip(&logs) {
        let eot = log.turns("EndOfTurn");
        let last = log
            .msgs
            .iter()
            .filter_map(|m| m.1["audio_window_end"].as_f64())
            .fold(0.0, f64::max);
        eprintln!(
            "{enc} {rate}: {:?}, EndOfTurn audio_window_end {:.2}, last {last:.2}",
            eot.first().map(|m| text(m.1)),
            end(log)
        );
        check_turn(log, 0);
        // 5 s of clip and 2 s of silence are 7 s at any rate
        assert!(
            (6.0..=7.2).contains(&last),
            "{enc} {rate}: last audio_window_end {last}"
        );
        assert!(
            (end(log) - end(&logs[0])).abs() < 0.3,
            "{enc} {rate}: EndOfTurn at {}",
            end(log)
        );
    }
}

#[test]
fn lid_names_a_language_outside_the_hint() {
    let srv = start(&[("TINYASR_LID", "models/tinylid.bin")]);
    let mut ws = connect(
        srv.port,
        &Q.replace("language_hint=ru", "language_hint=en"),
        None,
    )
    .unwrap();
    let mut log = Log::default();
    log.turn(&mut ws, &clip(), 4.0);
    log.close(&mut ws);
    let eot = log.turns("EndOfTurn");
    assert!(!eot.is_empty(), "no EndOfTurn");
    for (_, m) in &eot {
        assert_eq!(
            (&m["languages"], &m["languages_hinted"]),
            (&serde_json::json!(["ru"]), &serde_json::json!(["en"])),
            "{m}"
        );
    }
    eprintln!(
        "lid: end-of-turn after speech end {:?}",
        eot[0].0 - log.speech_end[0]
    );
}

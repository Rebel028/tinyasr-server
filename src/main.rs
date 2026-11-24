//! Streaming speech-to-text over a websocket: a subset of the Deepgram Flux protocol (`GET /v2/listen`) on tinyasr.
//! One connection = a socket thread (reads audio, writes messages and heartbeats) and a worker thread (the session).
//! A bounded audio queue between them keeps the decode off the socket read path.

mod turns;

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, Role};
use tungstenite::{Error as WsError, Message, WebSocket};
use turns::{Msg, Turns, list, q};

const SR: usize = 16000;
/// An `Update` goes out when nothing else did for this long (clients read the steady stream as liveness).
const HEARTBEAT: Duration = Duration::from_millis(200);
/// Socket read timeout: how often the socket thread looks at outgoing messages.
const POLL: Duration = Duration::from_millis(10);
/// Recent audio kept for tinylid (it reads at most 30 s).
const LID_KEEP: usize = 30 * SR;
/// The start of a turn given to tinylid: it costs ~30 ms per second of audio (i9-10900KF), so 3 s keep it under 100 ms.
const LID_MAX: usize = 3 * SR;

struct Cfg {
    models: tinyasr::Models,
    lid: Option<tinylid::Model>, // TINYASR_LID: the language of each EndOfTurn from its audio
    langs: Vec<String>,
    beam: usize,
    token: Option<String>,
    max: usize,
    queue: usize, // samples
    trace: bool,  // TINYASR_TRACE=1: one timing line per second per session
    active: AtomicUsize,
    ids: AtomicU64,
}

struct Params {
    langs: Vec<String>,
    hints: Vec<String>, // as the client sent them; empty when `langs` is the default
    thr: f32,
    timeout_ms: u32,
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| d.to_string())
}

fn num<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse()
                .unwrap_or_else(|_| panic!("{k}: not a number: {v}"))
        })
        .unwrap_or(d)
}

fn main() {
    let t0 = Instant::now();
    let asr =
        std::fs::read(env("TINYASR_MODEL", "/models/en-ru-uk.tzf")).expect("read TINYASR_MODEL");
    let vad =
        std::fs::read(env("TINYASR_VAD", "/models/silero_vad.onnx")).expect("read TINYASR_VAD");
    let models = tinyasr::Models::load(tinyasr::Files {
        asr: &asr,
        vad: &vad,
    })
    .expect("load models");
    drop((asr, vad));
    let lid_path = std::env::var("TINYASR_LID").ok().filter(|p| !p.is_empty());
    let lid = lid_path.as_ref().map(|p| {
        tinylid::Model::from_bytes(&std::fs::read(p).expect("read TINYASR_LID"))
            .expect("load TINYASR_LID")
    });
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let langs: Vec<String> = env("TINYASR_LANGS", "ru")
        .split(',')
        .map(|l| l.trim().to_ascii_lowercase())
        .collect();
    for l in &langs {
        assert!(
            models.langs().contains(&l.as_str()),
            "TINYASR_LANGS: {l} is not in the model ({:?})",
            models.langs()
        );
    }
    let cfg = Arc::new(Cfg {
        lid,
        langs,
        beam: num("TINYASR_BEAM", tinyasr::DEFAULT_BEAM),
        token: std::env::var("TINYASR_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
        max: num("TINYASR_MAX_SESSIONS", cores),
        queue: num("TINYASR_QUEUE_MS", 3000usize) * SR / 1000,
        trace: env("TINYASR_TRACE", "0") == "1",
        active: AtomicUsize::new(0),
        ids: AtomicU64::new(1),
        models,
    });
    let port = env("PORT", "8000");
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).expect("bind PORT");
    eprintln!(
        "tinyasr-server {} listening on :{port}, models loaded in {} ms, model langs {:?}, default langs {:?}, beam {}, max sessions {}, queue {} ms, auth {}, lid {}",
        env!("CARGO_PKG_VERSION"),
        t0.elapsed().as_millis(),
        cfg.models.langs(),
        cfg.langs,
        cfg.beam,
        cfg.max,
        cfg.queue * 1000 / SR,
        if cfg.token.is_some() { "token" } else { "off" },
        lid_path.as_deref().unwrap_or("off")
    );
    for s in listener.incoming().flatten() {
        let cfg = cfg.clone();
        std::thread::spawn(move || conn(cfg, s));
    }
}

fn conn(cfg: Arc<Cfg>, mut s: TcpStream) {
    let _ = s.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = s.set_nodelay(true);
    let Some((head, rest)) = read_head(&mut s) else {
        return;
    };
    let Some(req) = Req::parse(&head) else {
        return err(&mut s, 400, "BAD_REQUEST", "Malformed request.");
    };
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/health") => http(&mut s, 200, "{\"status\":\"ok\"}"),
        ("GET", "/v2/listen") => listen(cfg, s, &req, rest),
        _ => err(&mut s, 404, "NOT_FOUND", "Not found."),
    }
}

/// Decrements the session count when the connection thread ends, panics included.
struct Slot<'a>(&'a AtomicUsize);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn listen(cfg: Arc<Cfg>, mut s: TcpStream, req: &Req, rest: Vec<u8>) {
    let peer = s.peer_addr().map_or("?".into(), |a| a.to_string());
    if let Some(t) = &cfg.token {
        let ok = req
            .header("authorization")
            .and_then(|h| h.trim().split_once(' '))
            .is_some_and(|(scheme, v)| {
                (scheme.eq_ignore_ascii_case("token") || scheme.eq_ignore_ascii_case("bearer"))
                    && same(v.trim().as_bytes(), t.as_bytes())
            });
        if !ok {
            eprintln!("refused 401 peer={peer}");
            return err(&mut s, 401, "INVALID_AUTH", "Invalid credentials.");
        }
    }
    let p = match params(&cfg, req) {
        Ok(p) => p,
        Err(m) => return err(&mut s, 400, "INVALID_QUERY_PARAMETER", &m),
    };
    let Some(key) = req.header("sec-websocket-key").filter(|_| {
        req.header("upgrade")
            .is_some_and(|u| u.eq_ignore_ascii_case("websocket"))
    }) else {
        return err(
            &mut s,
            426,
            "UPGRADE_REQUIRED",
            "This endpoint takes a websocket upgrade.",
        );
    };
    if cfg.active.fetch_add(1, Ordering::SeqCst) >= cfg.max {
        cfg.active.fetch_sub(1, Ordering::SeqCst);
        eprintln!("refused 503 peer={peer}: {} sessions running", cfg.max);
        return err(
            &mut s,
            503,
            "TOO_MANY_SESSIONS",
            "All recognition sessions are busy, try again later.",
        );
    }
    let _slot = Slot(&cfg.active);
    let rid = uuid();
    let accept = tungstenite::handshake::derive_accept_key(key.as_bytes());
    let hs = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\ndg-request-id: {rid}\r\n\r\n"
    );
    if s.write_all(hs.as_bytes()).is_err() {
        return;
    }
    session(&cfg, s, rest, p, &rid, &peer);
}

fn params(cfg: &Cfg, req: &Req) -> Result<Params, String> {
    let (mut enc, mut rate, mut langs, mut thr, mut timeout_ms) =
        (None, None, vec![], 0.7f32, 5000u32);
    let mine = cfg.models.langs();
    for (k, v) in &req.query {
        match k.as_str() {
            "encoding" => enc = Some(v.as_str()),
            "sample_rate" => rate = Some(v.as_str()),
            "language_hint" => {
                for h in v.split(',') {
                    // ru, ru-RU, ru_RU -> ru
                    let c = h
                        .trim()
                        .split(['-', '_'])
                        .next()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if !mine.contains(&c.as_str()) {
                        return Err(format!(
                            "Unsupported language_hint {h:?}; this model knows {}.",
                            mine.join(", ")
                        ));
                    }
                    if !langs.contains(&c) {
                        langs.push(c);
                    }
                }
            }
            "eot_threshold" => {
                thr = v
                    .parse()
                    .ok()
                    .filter(|t| (0.0..=1.0).contains(t))
                    .ok_or("eot_threshold must be a number from 0 to 1.")?
            }
            "eot_timeout_ms" => {
                timeout_ms = v
                    .parse()
                    .ok()
                    .filter(|t| (100..=60000).contains(t))
                    .ok_or("eot_timeout_ms must be an integer from 100 to 60000.")?
            }
            _ => {} // model, keyterm, numerals and the rest: accepted and ignored
        }
    }
    if enc != Some("linear16") {
        return Err("Only encoding=linear16 (PCM16 little-endian, mono) is supported.".into());
    }
    if rate.is_some_and(|r| r != "16000") {
        return Err("Only sample_rate=16000 is supported.".into());
    }
    let hints = langs.clone();
    if langs.is_empty() {
        langs = cfg.langs.clone();
    }
    Ok(Params {
        langs,
        hints,
        thr,
        timeout_ms,
    })
}

fn session(cfg: &Arc<Cfg>, s: TcpStream, rest: Vec<u8>, p: Params, rid: &str, peer: &str) {
    let id = cfg.ids.fetch_add(1, Ordering::Relaxed);
    let t0 = Instant::now();
    eprintln!(
        "session {id} start peer={peer} request_id={rid} langs={} eot_threshold={} eot_timeout_ms={}",
        p.langs.join(","),
        p.thr,
        p.timeout_ms
    );
    let _ = s.set_read_timeout(Some(POLL));
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    let hinted = list(&p.hints.iter().map(String::as_str).collect::<Vec<_>>());
    let mut ws = WebSocket::from_partially_read(s, rest, Role::Server, None);
    let audio = Arc::new(Audio::new(cfg.queue));
    let pos = Arc::new(AtomicU64::new(0)); // stream samples the worker has processed
    let sent = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]); // messages, bytes; counted only with trace on
    let (tx, rx) = mpsc::channel::<Msg>();
    let worker = {
        let (cfg, audio, pos, sent) = (cfg.clone(), audio.clone(), pos.clone(), sent.clone());
        std::thread::Builder::new()
            .name(format!("session-{id}"))
            .spawn(move || work(&cfg, &p, &audio, &pos, &tx, &sent))
    };
    let Ok(worker) = worker else { return };
    let mut worker = Some(worker);
    let (mut turns, mut end) = (None, "client gone");
    let mut ok = ws
        .send(Message::text(format!(
            "{{\"type\":\"Connected\",\"request_id\":\"{rid}\",\"sequence_id\":0}}"
        )))
        .is_ok();
    let mut seq = 1u64;
    let mut hb = Msg {
        event: "Update",
        turn_index: 0,
        start: 0.0,
        end: 0.0,
        transcript: String::new(),
        conf: 0.0,
        trigger: None,
        lang: String::new(),
        silence: None,
    };
    let mut last = Instant::now();
    // (lag, dropped) in ms, and pos
    let stamp = || {
        let ((got, dropped), p) = (audio.stats(), pos.load(Ordering::Relaxed));
        let ms = |v: u64| v * 1000 / SR as u64;
        (ms(got.saturating_sub(p)), ms(dropped), p)
    };
    let mut odd = None;
    let mut closing: Option<Instant> = None;
    while ok {
        match ws.read() {
            Ok(Message::Binary(b)) => audio.push(pcm(&b, &mut odd)),
            Ok(Message::Text(t)) => {
                if msg_type(&t) == Some("CloseStream") {
                    audio.set(State::Finish);
                }
            }
            Ok(Message::Close(_)) if closing.is_none() => end = "client closed",
            Ok(_) => {}
            Err(WsError::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break,
            Err(_) => {
                end = "socket error";
                break;
            }
        }
        if let Some(at) = closing {
            if at.elapsed() > Duration::from_secs(2) {
                break;
            }
        } else {
            loop {
                match rx.try_recv() {
                    Ok(m) => {
                        let (lag, dropped, _) = stamp();
                        let j = m.json(rid, &hinted, seq, lag, dropped);
                        if cfg.trace {
                            sent[0].fetch_add(1, Ordering::Relaxed);
                            sent[1].fetch_add(j.len() as u64, Ordering::Relaxed);
                        }
                        ok &= ws.send(Message::text(j)).is_ok();
                        seq += 1;
                        hb = m.heartbeat();
                        last = Instant::now();
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        // the worker is done: CloseStream finished it, or it panicked
                        let (frame, why) = match worker.take().map(|h| h.join()) {
                            Some(Ok(n)) => {
                                turns = Some(n);
                                (
                                    CloseFrame {
                                        code: CloseCode::Normal,
                                        reason: "".into(),
                                    },
                                    "close stream",
                                )
                            }
                            _ => {
                                let e = format!(
                                    "{{\"type\":\"Error\",\"sequence_id\":{seq},\"code\":\"INTERNAL_SERVER_ERROR\",\"description\":\"The recognition session failed.\"}}"
                                );
                                let _ = ws.send(Message::text(e));
                                (
                                    CloseFrame {
                                        code: CloseCode::Error,
                                        reason: "session failed".into(),
                                    },
                                    "session failed",
                                )
                            }
                        };
                        end = why;
                        ok &= ws.close(Some(frame)).is_ok();
                        closing = Some(Instant::now());
                        break;
                    }
                }
            }
            if closing.is_none() && ok && last.elapsed() >= HEARTBEAT {
                let (lag, dropped, p) = stamp();
                hb.end = p as f64 / SR as f64;
                let j = hb.json(rid, &hinted, seq, lag, dropped);
                if cfg.trace {
                    sent[0].fetch_add(1, Ordering::Relaxed);
                    sent[1].fetch_add(j.len() as u64, Ordering::Relaxed);
                }
                ok &= ws.send(Message::text(j)).is_ok();
                seq += 1;
                last = Instant::now();
            }
        }
        if ok && ws.flush().is_err() && closing.is_none() {
            end = "socket error";
            break;
        }
    }
    if !ok && closing.is_none() {
        end = "socket error";
    }
    audio.set(State::Abort);
    if let Some(h) = worker.take() {
        turns = h.join().ok();
        if turns.is_none() {
            end = "session failed";
        }
    }
    let (got, dropped) = audio.stats();
    eprintln!(
        "session {id} end reason={end:?} duration_s={:.1} audio_s={:.1} dropped_s={:.2} turns={}",
        t0.elapsed().as_secs_f64(),
        got as f64 / SR as f64,
        dropped as f64 / SR as f64,
        turns.map_or("?".into(), |n| n.to_string())
    );
}

/// The session worker: audio from the queue into the library, turn messages out. Returns the number of turns ended.
fn work(
    cfg: &Cfg,
    p: &Params,
    audio: &Audio,
    pos: &AtomicU64,
    tx: &mpsc::Sender<Msg>,
    sent: &[AtomicU64; 2],
) -> u32 {
    let codes: Vec<&str> = p.langs.iter().map(String::as_str).collect();
    let mut s = tinyasr::Session::new(
        &cfg.models,
        &codes,
        tinyasr::Options {
            beam: cfg.beam,
            ..Default::default()
        },
    );
    let mut t = Turns::new(p.thr, p.timeout_ms, s.lang());
    let (mut lib, mut out) = (0usize, vec![]);
    let mut recent: VecDeque<f32> = VecDeque::new(); // the last LID_KEEP library samples, only with tinylid on
    // trace: push count, push time sum and max this second; open sentence words and its start (library samples); turn chars
    let (t0, mut tlog) = (Instant::now(), Instant::now());
    let (mut pn, mut psum, mut pmax) = (0u32, Duration::ZERO, Duration::ZERO);
    let (mut open_w, mut open_at, mut turn_c) = (0usize, 0usize, 0usize);
    loop {
        let (evs, off, fin) = match audio.pop() {
            Pop::Audio(c, off) => {
                lib += c.len();
                if cfg.lid.is_some() {
                    recent.extend(&c);
                    recent.drain(..recent.len().saturating_sub(LID_KEEP));
                }
                let t1 = cfg.trace.then(Instant::now);
                let evs = s.push(&c);
                if let Some(t1) = t1 {
                    let d = t1.elapsed();
                    (pn, psum, pmax) = (pn + 1, psum + d, pmax.max(d));
                }
                (evs, off, false)
            }
            Pop::Finish(off) => (s.finish(), off, true),
            Pop::Abort => return t.count(),
        };
        let (ps, os) = (lib as f64 / SR as f64, off as f64 / SR as f64);
        for e in evs {
            if cfg.trace {
                match &e {
                    tinyasr::Event::Partial { open, tail } => open_w = open.len() + tail.len(),
                    tinyasr::Event::Commit(_) => (open_w, open_at) = (0, lib),
                    _ => {}
                }
            }
            t.on(e, ps, os, &mut out);
        }
        if fin {
            t.finish(ps, os, &mut out);
        } else {
            t.tick(ps, os, &mut out);
        }
        pos.store(lib as u64 + off, Ordering::Relaxed);
        if let Some(lid) = &cfg.lid {
            for m in out.iter_mut().filter(|m| m.event == "EndOfTurn") {
                // the turn start in library samples (drops inside the turn make this a little early)
                let from = ((m.start - os) * SR as f64).max(0.0) as usize;
                let skip = recent.len().saturating_sub(lib.saturating_sub(from));
                let x: Vec<f32> = recent.iter().skip(skip).take(LID_MAX).copied().collect();
                if let Some((code, _)) = lid.top(&lid.predict(&x, tinylid::Mode::Fast), 1).first() {
                    m.lang = code.to_string();
                }
            }
        }
        if cfg.trace {
            if let Some(m) = out.last() {
                turn_c = if m.event == "EndOfTurn" {
                    0
                } else {
                    m.transcript.chars().count()
                };
            }
            if tlog.elapsed() >= Duration::from_secs(1) {
                let (recv, queued, dropped) = {
                    let q = audio.lock();
                    (q.received, q.len, q.dropped)
                };
                // statm field 2: resident pages; 4 KiB pages assumed
                let rss = std::fs::read_to_string("/proc/self/statm")
                    .ok()
                    .and_then(|m| m.split_whitespace().nth(1)?.parse::<u64>().ok())
                    .map_or(-1.0, |p| (p * 4096) as f64 / 1048576.0);
                let secs = |v: usize| v as f64 / SR as f64;
                eprintln!(
                    "{} trace t_s={:.1} push_n={pn} push_ms={:.1} push_max_ms={:.1} proc_s={:.2} recv_s={:.2} queue_s={:.2} dropped_s={:.2} open_words={open_w} open_s={:.2} turn_chars={turn_c} msgs={} msg_bytes={} rss_mb={rss:.1}",
                    std::thread::current()
                        .name()
                        .unwrap_or("session-?")
                        .replace('-', " "),
                    t0.elapsed().as_secs_f64(),
                    psum.as_secs_f64() * 1e3,
                    pmax.as_secs_f64() * 1e3,
                    (lib as u64 + off) as f64 / SR as f64,
                    recv as f64 / SR as f64,
                    secs(queued),
                    dropped as f64 / SR as f64,
                    secs(lib - open_at),
                    sent[0].swap(0, Ordering::Relaxed),
                    sent[1].swap(0, Ordering::Relaxed),
                );
                (tlog, pn, psum, pmax) = (Instant::now(), 0, Duration::ZERO, Duration::ZERO);
            }
        }
        for m in out.drain(..) {
            if tx.send(m).is_err() {
                return t.count();
            }
        }
        if fin {
            return t.count();
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Run,
    Finish,
    Abort,
}

enum Pop {
    /// A chunk and the samples dropped before it.
    Audio(Vec<f32>, u64),
    Finish(u64),
    Abort,
}

struct AudioQ {
    chunks: VecDeque<Vec<f32>>,
    len: usize,
    received: u64,
    dropped: u64,
    state: State,
}

/// Bounded audio queue: on overflow the oldest audio goes.
struct Audio {
    q: Mutex<AudioQ>,
    cv: Condvar,
    cap: usize,
}

impl Audio {
    fn new(cap: usize) -> Audio {
        Audio {
            q: Mutex::new(AudioQ {
                chunks: VecDeque::new(),
                len: 0,
                received: 0,
                dropped: 0,
                state: State::Run,
            }),
            cv: Condvar::new(),
            cap,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AudioQ> {
        self.q.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn push(&self, c: Vec<f32>) {
        let mut q = self.lock();
        if q.state != State::Run || c.is_empty() {
            return;
        }
        q.received += c.len() as u64;
        q.len += c.len();
        q.chunks.push_back(c);
        while q.len > self.cap {
            let Some(o) = q.chunks.pop_front() else { break };
            q.len -= o.len();
            q.dropped += o.len() as u64;
        }
        drop(q);
        self.cv.notify_one();
    }

    fn set(&self, s: State) {
        let mut q = self.lock();
        if q.state != State::Abort {
            q.state = s;
        }
        drop(q);
        self.cv.notify_one();
    }

    fn pop(&self) -> Pop {
        let mut q = self.lock();
        loop {
            if q.state == State::Abort {
                return Pop::Abort;
            }
            if let Some(c) = q.chunks.pop_front() {
                q.len -= c.len();
                return Pop::Audio(c, q.dropped);
            }
            if q.state == State::Finish {
                return Pop::Finish(q.dropped);
            }
            q = self.cv.wait(q).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// (samples received, samples dropped)
    fn stats(&self) -> (u64, u64) {
        let q = self.lock();
        (q.received, q.dropped)
    }
}

/// PCM16 LE to f32; an odd byte waits for the next frame.
fn pcm(b: &[u8], odd: &mut Option<u8>) -> Vec<f32> {
    let mut bytes: Vec<u8> = odd.take().into_iter().chain(b.iter().copied()).collect();
    if bytes.len() % 2 == 1 {
        *odd = bytes.pop();
    }
    let (pairs, _) = bytes.as_chunks::<2>();
    pairs
        .iter()
        .map(|p| i16::from_le_bytes(*p) as f32 / 32768.0)
        .collect()
}

/// The `type` of a JSON control message.
/// shortcut: a substring scan, not a JSON parser; enough for {"type":"CloseStream"}, use serde_json if messages grow.
fn msg_type(t: &str) -> Option<&str> {
    let r = t[t.find("\"type\"")? + 6..]
        .trim_start()
        .strip_prefix(':')?
        .trim_start()
        .strip_prefix('"')?;
    r.split('"').next()
}

struct Req {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
}

impl Req {
    fn parse(head: &str) -> Option<Req> {
        let mut lines = head.split("\r\n");
        let mut rl = lines.next()?.split_whitespace();
        let (method, target) = (rl.next()?.to_string(), rl.next()?);
        let (path, qs) = target.split_once('?').unwrap_or((target, ""));
        let query = qs
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| kv.split_once('=').unwrap_or((kv, "")))
            .map(|(k, v)| (unescape(k), unescape(v)))
            .collect();
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        Some(Req {
            method,
            path: path.to_string(),
            query,
            headers,
        })
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|h| h.0 == name)
            .map(|h| h.1.as_str())
    }
}

fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let (mut o, mut i) = (Vec::with_capacity(b.len()), 0);
    while i < b.len() {
        let hex = |j: usize| b.get(j).and_then(|c| (*c as char).to_digit(16));
        match b[i] {
            b'+' => o.push(b' '),
            b'%' if hex(i + 1).is_some() && hex(i + 2).is_some() => {
                o.push((hex(i + 1).unwrap() * 16 + hex(i + 2).unwrap()) as u8);
                i += 2;
            }
            c => o.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&o).into_owned()
}

/// The HTTP head (up to the blank line) and any bytes read after it.
fn read_head(s: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let (mut buf, mut chunk) = (Vec::new(), [0u8; 2048]);
    loop {
        let n = s.read(&mut chunk).ok().filter(|&n| n > 0)?;
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let rest = buf.split_off(i + 4);
            return Some((String::from_utf8_lossy(&buf).into_owned(), rest));
        }
        if buf.len() > 16384 {
            return None;
        }
    }
}

fn http(s: &mut TcpStream, code: u16, body: &str) {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        426 => "Upgrade Required",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let _ = write!(
        s,
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn err(s: &mut TcpStream, code: u16, err_code: &str, msg: &str) {
    http(
        s,
        code,
        &format!("{{\"err_code\":\"{err_code}\",\"err_msg\":{}}}", q(msg)),
    );
}

/// Constant-time comparison of the token.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |x, (p, q)| x | (p ^ q)) == 0
}

fn uuid() -> String {
    let mut b = [0u8; 16];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .is_err()
    {
        b = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes();
    }
    b[6] = b[6] & 0x0f | 0x40;
    b[8] = b[8] & 0x3f | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_query() {
        let r = Req::parse("GET /v2/listen?model=flux-general-multi&encoding=linear16&language_hint=ru%2DRU&x HTTP/1.1\r\nAuthorization: Token abc\r\n").unwrap();
        assert_eq!((r.method.as_str(), r.path.as_str()), ("GET", "/v2/listen"));
        assert_eq!(r.query[2], ("language_hint".into(), "ru-RU".into()));
        assert_eq!(r.query[3], ("x".into(), "".into()));
        assert_eq!(r.header("authorization"), Some("Token abc"));
        assert_eq!(unescape("a+b%2"), "a b%2");
    }

    #[test]
    fn pcm_carries_an_odd_byte_to_the_next_frame() {
        let mut odd = None;
        assert_eq!(pcm(&[0x00, 0x80, 0xff], &mut odd), vec![-1.0]);
        assert_eq!(pcm(&[0x7f], &mut odd), vec![32767.0 / 32768.0]);
        assert_eq!(odd, None);
    }

    #[test]
    fn control_message_type() {
        assert_eq!(
            msg_type(r#"{ "type" : "CloseStream" }"#),
            Some("CloseStream")
        );
        assert_eq!(msg_type("{}"), None);
    }

    #[test]
    fn token_compare() {
        assert!(same(b"abc", b"abc") && !same(b"abc", b"abd") && !same(b"ab", b"abc"));
    }

    #[test]
    fn uuid_is_v4() {
        let u = uuid();
        let b = u.as_bytes();
        assert_eq!(b.len(), 36, "{u}");
        assert!([8, 13, 18, 23].iter().all(|&i| b[i] == b'-'), "{u}");
        assert!(
            b[14] == b'4' && matches!(b[19], b'8' | b'9' | b'a' | b'b'),
            "{u}"
        );
    }

    #[test]
    fn queue_drops_the_oldest() {
        let a = Audio::new(4);
        a.push(vec![1.0; 2]);
        a.push(vec![2.0; 2]);
        a.push(vec![3.0; 2]);
        assert_eq!(a.stats(), (6, 2));
        assert!(matches!(a.pop(), Pop::Audio(c, 2) if c[0] == 2.0));
        a.set(State::Finish);
        assert!(matches!(a.pop(), Pop::Audio(c, 2) if c[0] == 3.0));
        assert!(matches!(a.pop(), Pop::Finish(2)));
        a.set(State::Abort);
        a.set(State::Run);
        assert!(matches!(a.pop(), Pop::Abort));
    }
}

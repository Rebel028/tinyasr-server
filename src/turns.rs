//! Turn tracking: library events in, Flux `TurnInfo` messages out. Pure logic, no I/O.

use tinyasr::{Event, Word};

/// Silence at the end-of-turn head's first and second look at a pause (the library's `PAD` and twice it, 40 ms frames).
const EOU_AT: f64 = 0.24;
const EOU_LATE: f64 = 0.48;

/// One `TurnInfo` message. Times are seconds of the client's audio stream (dropped audio included).
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub event: &'static str,
    pub turn_index: u32,
    pub start: f64,
    pub end: f64,
    pub transcript: String,
    pub conf: f32,
    pub trigger: Option<&'static str>,
    /// The detected language code; not sent while the transcript is empty.
    pub lang: String,
    /// `EndOfTurn` only: s of silence since the pause began, when the library reported the pause.
    pub silence: Option<f64>,
}

impl Msg {
    /// `hinted`: the session's `language_hint` codes as a JSON array (see `list`).
    /// `lag_ms`: audio received minus audio processed; `dropped_ms`: audio the queue dropped in this session.
    pub fn json(
        &self,
        request_id: &str,
        hinted: &str,
        seq: u64,
        lag_ms: u64,
        dropped_ms: u64,
    ) -> String {
        let langs = if self.transcript.is_empty() {
            "[]".into()
        } else {
            list(&[&self.lang])
        };
        let trigger = self
            .trigger
            .map_or(String::new(), |t| format!(",\"trigger\":\"{t}\""));
        let silence = self.silence.map_or(String::new(), |s| {
            format!(",\"silence_ms\":{:.0}", s * 1000.0)
        });
        format!(
            "{{\"type\":\"TurnInfo\",\"request_id\":\"{request_id}\",\"sequence_id\":{seq},\"event\":\"{}\",\"turn_index\":{},\"audio_window_start\":{:.2},\"audio_window_end\":{:.2},\"transcript\":{},\"words\":[],\"languages\":{langs},\"languages_hinted\":{hinted},\"end_of_turn_confidence\":{:.3}{trigger}{silence},\"lag_ms\":{lag_ms},\"dropped_ms\":{dropped_ms}}}",
            self.event,
            self.turn_index,
            self.start,
            self.end,
            q(&self.transcript),
            self.conf
        )
    }

    /// The idle `Update` that follows this message: after an `EndOfTurn`, the next turn with no text.
    pub fn heartbeat(&self) -> Msg {
        if self.event == "EndOfTurn" {
            Msg {
                event: "Update",
                turn_index: self.turn_index + 1,
                start: self.end,
                transcript: String::new(),
                conf: 0.0,
                trigger: None,
                lang: self.lang.clone(),
                silence: None,
                ..*self
            }
        } else {
            Msg {
                event: "Update",
                trigger: None,
                silence: None,
                transcript: self.transcript.clone(),
                lang: self.lang.clone(),
                ..*self
            }
        }
    }
}

pub struct Turns {
    thr: f32,
    timeout: f64,
    index: u32,
    open: bool,
    committed: String,
    partial: String,
    start: f64,                  // stream s where the turn (or the idle gap) began
    last_word: f64,              // library s: end of the newest word of the turn
    pending: Option<(f32, f64)>, // end-of-turn head said yes at a pause: (p, library s of that pause)
    pause: Option<f64>, // library s where the silence the head judged began; None once speech went on
    head: bool,         // the model has the end-of-turn head (an Eou came)
    last_p: f32,
    skip_commit: bool, // the turn was closed on open words: their later Commit is already reported
    lang: String,      // the library's current language
}

impl Turns {
    pub fn new(thr: f32, timeout_ms: u32, lang: &str) -> Turns {
        Turns {
            thr,
            timeout: timeout_ms as f64 / 1000.0,
            index: 0,
            open: false,
            committed: String::new(),
            partial: String::new(),
            start: 0.0,
            last_word: 0.0,
            pending: None,
            pause: None,
            head: false,
            last_p: 0.0,
            skip_commit: false,
            lang: lang.into(),
        }
    }

    /// Turns ended so far.
    pub fn count(&self) -> u32 {
        self.index
    }

    /// One library event. `pos`: library s pushed so far; `off`: s of audio dropped before it.
    pub fn on(&mut self, e: Event, pos: f64, off: f64, out: &mut Vec<Msg>) {
        match e {
            Event::Partial { open, tail } => {
                let ws: Vec<&Word> = open.iter().chain(&tail).collect();
                let text = join(&ws);
                if text == self.partial {
                    return;
                }
                self.partial = text;
                if self.partial.trim().is_empty() {
                    return;
                }
                self.words(&ws);
                self.begin(&ws, pos, off, out);
                out.push(self.msg("Update", pos, off, 0.0, None));
            }
            Event::Commit(ws) => {
                self.partial.clear();
                let ws: Vec<&Word> = ws.iter().collect();
                let text = join(&ws);
                if std::mem::take(&mut self.skip_commit) || text.trim().is_empty() {
                    return;
                }
                if !self.committed.is_empty() {
                    self.committed.push(' ');
                }
                self.committed.push_str(&text);
                self.words(&ws);
                self.begin(&ws, pos, off, out);
                // the library can freeze an earlier sentence first, in the same push: it does so only for a sentence end
                // it heard 1.5 s before, so a Commit that ends more than 1 s before the pause leaves words of the turn
                let reaches = |p0: f64| ws.last().is_some_and(|w| w.end as f64 > p0 - 1.0);
                match self.pending {
                    Some((p, _)) if self.pause.is_none_or(reaches) => {
                        self.end(p, "model", pos, off, out)
                    }
                    _ => out.push(self.msg("Update", pos, off, self.last_p, None)),
                }
            }
            Event::Eou { at, p } => {
                let at = at as f64;
                self.last_p = p;
                self.head = true;
                // the head runs at 0.24 s of silence, and after a "no" at 0.48 s of the same pause
                if !self
                    .pause
                    .is_some_and(|p0| (at - EOU_LATE - p0).abs() < 0.02)
                {
                    self.pause = Some(at - EOU_AT);
                }
                if self.open {
                    self.pending = (p >= self.thr).then_some((p, at));
                }
            }
            Event::Language(l) => self.lang = l,
        }
    }

    /// After each push: the silence timeout. Silence starts where the library's pause began (as Flux, speech resets
    /// it); a model without the end-of-turn head reports no pause, so there it starts at the end of the newest word.
    /// shortcut: speech that resumes and shows no word yet leaves the old pause running (the library does not report
    /// the VAD), and a session counts as headless until its first Eou; upgrade when the library reports both.
    pub fn tick(&mut self, pos: f64, off: f64, out: &mut Vec<Msg>) {
        let from = if self.head {
            self.pause
        } else {
            Some(self.last_word)
        };
        if self.open && from.is_some_and(|q| pos - q >= self.timeout) {
            self.skip_commit = !self.partial.trim().is_empty();
            self.end(self.last_p, "timeout", pos, off, out);
        }
    }

    /// End of the stream (after the library's `finish` events): close the open turn.
    pub fn finish(&mut self, pos: f64, off: f64, out: &mut Vec<Msg>) {
        if self.open {
            self.end(self.last_p, "manual", pos, off, out);
        }
    }

    fn words(&mut self, ws: &[&Word]) {
        let newest = ws
            .iter()
            .map(|w| w.end as f64)
            .fold(self.last_word, f64::max);
        // a word that ends after the pause the head judged means the speaker went on
        if self.pending.is_some_and(|(_, at)| newest > at) {
            self.pending = None;
        }
        if self.pause.is_some_and(|p0| newest > p0 + EOU_AT) {
            self.pause = None;
        }
        self.last_word = newest;
    }

    fn begin(&mut self, ws: &[&Word], pos: f64, off: f64, out: &mut Vec<Msg>) {
        if self.open {
            return;
        }
        self.open = true;
        self.pending = None;
        self.start = ws.first().map_or(pos, |w| w.start as f64) + off;
        out.push(self.msg("StartOfTurn", pos, off, 0.0, None));
    }

    fn end(&mut self, p: f32, trigger: &'static str, pos: f64, off: f64, out: &mut Vec<Msg>) {
        let mut m = self.msg("EndOfTurn", pos, off, p, Some(trigger));
        m.silence = self.pause.map(|p0| pos - p0);
        out.push(m);
        self.index += 1;
        self.open = false;
        self.committed.clear();
        self.partial.clear();
        self.pending = None;
        self.pause = None;
        self.last_p = 0.0;
        self.start = pos + off;
    }

    fn msg(
        &self,
        event: &'static str,
        pos: f64,
        off: f64,
        conf: f32,
        trigger: Option<&'static str>,
    ) -> Msg {
        let t = match (self.committed.is_empty(), self.partial.trim().is_empty()) {
            (_, true) => self.committed.clone(),
            (true, false) => self.partial.clone(),
            (false, false) => format!("{} {}", self.committed, self.partial),
        };
        Msg {
            event,
            turn_index: self.index,
            start: self.start,
            end: pos + off,
            transcript: t.trim().to_string(),
            conf,
            trigger,
            lang: self.lang.clone(),
            silence: None,
        }
    }
}

fn join(ws: &[&Word]) -> String {
    let mut s = String::new();
    for w in ws {
        if !s.is_empty() && !w.join {
            s.push(' ');
        }
        s.push_str(&w.text);
    }
    s
}

/// A JSON array of strings.
pub fn list(v: &[&str]) -> String {
    format!("[{}]", v.iter().map(|s| q(s)).collect::<Vec<_>>().join(","))
}

pub fn q(s: &str) -> String {
    let mut o = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(t: &str, s: f32, e: f32) -> Word {
        Word {
            text: t.into(),
            start: s,
            end: e,
            join: false,
        }
    }

    fn part(ws: &[Word]) -> Event {
        Event::Partial {
            open: ws.to_vec(),
            tail: vec![],
        }
    }

    fn events(out: &[Msg]) -> Vec<(&str, u32, &str)> {
        out.iter()
            .map(|m| (m.event, m.turn_index, m.transcript.as_str()))
            .collect()
    }

    #[test]
    fn model_end_of_turn_waits_for_the_commit() {
        let mut t = Turns::new(0.7, 5000, "ru");
        let mut o = vec![];
        t.on(part(&[w("сервер", 0.2, 0.6)]), 0.8, 0.0, &mut o);
        t.on(
            part(&[w("сервер", 0.2, 0.6), w("упал", 0.7, 1.0)]),
            1.2,
            0.0,
            &mut o,
        );
        t.on(Event::Eou { at: 1.24, p: 0.8 }, 1.24, 0.0, &mut o);
        t.on(
            part(&[w("сервер", 0.2, 0.6), w("упал.", 0.7, 1.0)]),
            1.32,
            0.0,
            &mut o,
        ); // re-decode at the pause
        t.on(
            Event::Commit(vec![w("Сервер", 0.2, 0.6), w("упал.", 0.7, 1.0)]),
            1.48,
            0.0,
            &mut o,
        );
        t.on(part(&[]), 1.52, 0.0, &mut o);
        assert_eq!(
            events(&o),
            vec![
                ("StartOfTurn", 0, "сервер"),
                ("Update", 0, "сервер"),
                ("Update", 0, "сервер упал"),
                ("Update", 0, "сервер упал."),
                ("EndOfTurn", 0, "Сервер упал.")
            ]
        );
        assert_eq!((o[4].conf, o[4].trigger), (0.8, Some("model")));
        assert!((o[4].silence.unwrap() - 0.48).abs() < 1e-3);
        assert_eq!(o[4].heartbeat().turn_index, 1);
        t.on(part(&[w("да", 3.0, 3.2)]), 3.3, 0.5, &mut o);
        assert_eq!(
            events(&o[5..]),
            vec![("StartOfTurn", 1, "да"), ("Update", 1, "да")]
        );
        assert_eq!(o[5].start, 3.5);
        assert_eq!(t.count(), 1);
    }

    #[test]
    fn low_head_and_resumed_speech_keep_the_turn() {
        let mut t = Turns::new(0.7, 5000, "ru");
        let mut o = vec![];
        t.on(part(&[w("один", 0.1, 0.5)]), 0.6, 0.0, &mut o);
        t.on(Event::Eou { at: 0.8, p: 0.6 }, 0.8, 0.0, &mut o);
        t.on(Event::Commit(vec![w("Один.", 0.1, 0.5)]), 1.0, 0.0, &mut o); // the library ends the sentence at 0.5
        t.on(Event::Eou { at: 1.6, p: 0.9 }, 1.6, 0.0, &mut o);
        t.on(part(&[w("два", 1.7, 2.0)]), 2.1, 0.0, &mut o); // speech after the pause cancels the yes
        t.on(Event::Commit(vec![w("Два.", 1.7, 2.0)]), 2.2, 0.0, &mut o); // ". and the next sentence started" rule
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        assert_eq!(o.last().unwrap().transcript, "Один. Два.");
        t.on(Event::Eou { at: 2.24, p: 0.3 }, 2.24, 0.0, &mut o); // the pause after "два" (from 2.0)
        t.tick(6.9, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        t.tick(7.01, 0.0, &mut o);
        let m = o.last().unwrap();
        assert_eq!(
            (m.event, m.trigger, m.transcript.as_str()),
            ("EndOfTurn", Some("timeout"), "Один. Два.")
        );
    }

    #[test]
    fn timeout_on_open_words_skips_their_commit() {
        let mut t = Turns::new(0.7, 1000, "ru");
        let mut o = vec![];
        t.on(part(&[w("алло", 0.1, 0.5)]), 0.6, 0.0, &mut o);
        t.tick(1.5, 0.0, &mut o);
        assert_eq!(o.last().unwrap().event, "EndOfTurn");
        t.on(Event::Commit(vec![w("Алло.", 0.1, 0.5)]), 3.1, 0.0, &mut o);
        assert_eq!(o.last().unwrap().event, "EndOfTurn");
        t.finish(4.0, 0.0, &mut o);
        assert_eq!(t.count(), 1);
    }

    #[test]
    fn json_shape() {
        let m = Msg {
            event: "EndOfTurn",
            turn_index: 2,
            start: 1.0,
            end: 2.5,
            transcript: "a \"b\"".into(),
            conf: 0.75,
            trigger: Some("model"),
            lang: "ru".into(),
            silence: Some(0.48),
        };
        assert_eq!(
            m.json("r", &list(&["ru", "en"]), 7, 120, 40),
            r#"{"type":"TurnInfo","request_id":"r","sequence_id":7,"event":"EndOfTurn","turn_index":2,"audio_window_start":1.00,"audio_window_end":2.50,"transcript":"a \"b\"","words":[],"languages":["ru"],"languages_hinted":["ru","en"],"end_of_turn_confidence":0.750,"trigger":"model","silence_ms":480,"lag_ms":120,"dropped_ms":40}"#
        );
        let hb = m.heartbeat().json("r", "[]", 8, 0, 0);
        assert!(
            hb.ends_with(r#""end_of_turn_confidence":0.000,"lag_ms":0,"dropped_ms":0}"#),
            "{hb}"
        );
    }

    #[test]
    fn timeout_counts_silence_and_keeps_the_head_score() {
        let mut t = Turns::new(0.7, 1500, "ru");
        let mut o = vec![];
        t.on(part(&[w("да", 0.1, 0.4)]), 0.6, 0.0, &mut o);
        t.on(Event::Eou { at: 0.84, p: 0.3 }, 0.84, 0.0, &mut o); // pause from 0.6
        t.on(part(&[w("да.", 0.1, 0.4)]), 0.92, 0.0, &mut o); // a changed partial does not lose the score
        t.on(Event::Eou { at: 1.08, p: 0.6 }, 1.08, 0.0, &mut o); // the second look at the same pause
        t.tick(2.09, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        t.tick(2.11, 0.0, &mut o);
        let m = o.last().unwrap();
        assert_eq!(
            (m.event, m.trigger, m.conf),
            ("EndOfTurn", Some("timeout"), 0.6)
        );
        assert!((m.silence.unwrap() - 1.51).abs() < 1e-3);
    }

    #[test]
    fn timeout_waits_through_speech_with_late_words() {
        let mut t = Turns::new(0.5, 1500, "ru");
        let mut o = vec![];
        t.on(part(&[w("один", 0.1, 0.5)]), 0.8, 0.0, &mut o);
        t.on(Event::Eou { at: 0.84, p: 0.2 }, 0.84, 0.0, &mut o);
        t.on(
            part(&[w("один", 0.1, 0.5), w("два", 0.9, 1.3)]),
            2.0,
            0.0,
            &mut o,
        );
        // the speaker goes on; the next word shows 1.6 s after "два" ended (the old rule fired at 2.8)
        for i in 0..13 {
            t.tick(2.04 + 0.2 * i as f64, 0.0, &mut o);
        }
        t.on(
            part(&[w("один", 0.1, 0.5), w("два", 0.9, 1.3), w("три", 1.4, 3.0)]),
            4.6,
            0.0,
            &mut o,
        );
        t.tick(4.64, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
    }

    #[test]
    fn resumed_speech_clears_the_pause() {
        let mut t = Turns::new(0.7, 1500, "ru");
        let mut o = vec![];
        t.on(part(&[w("один", 0.1, 0.5)]), 0.8, 0.0, &mut o);
        t.on(Event::Eou { at: 1.0, p: 0.2 }, 1.0, 0.0, &mut o); // pause from 0.76
        t.on(
            part(&[w("один", 0.1, 0.5), w("два", 1.1, 1.5)]),
            1.8,
            0.0,
            &mut o,
        );
        t.tick(2.3, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        // speech with no word shown, then a new pause from 2.76: the old pause does not count
        t.on(Event::Eou { at: 3.0, p: 0.2 }, 3.0, 0.0, &mut o);
        t.tick(4.25, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        t.tick(4.27, 0.0, &mut o);
        assert_eq!(o.last().unwrap().trigger, Some("timeout"));
    }

    #[test]
    fn model_end_of_turn_waits_for_the_commit_that_reaches_the_pause() {
        // the real events of a clip with a pause mid-phrase (0.5 / 1500): two Commits in one push
        let mut t = Turns::new(0.5, 1500, "ru");
        let mut o = vec![];
        let first = [
            w("Кто", 10.04, 10.16),
            w("вы", 10.36, 10.4),
            w("такие?", 10.4, 10.44),
        ];
        t.on(part(&first), 10.76, 0.0, &mut o);
        let all = [
            w("Здравствуйте,", 9.12, 9.32),
            w("а", 9.76, 9.8),
            w("Кто", 10.12, 10.16),
            w("вы", 10.4, 10.56),
            w("такие?", 10.56, 10.6),
            w("Вы", 11.04, 11.2),
            w("ещё", 11.2, 11.36),
            w("на", 11.36, 11.4),
            w("связи?", 11.4, 11.44),
        ];
        t.on(part(&all), 11.72, 0.0, &mut o);
        t.on(
            Event::Eou {
                at: 12.36,
                p: 0.675,
            },
            12.36,
            0.0,
            &mut o,
        );
        let a = vec![
            w("Здравствуйте,", 9.12, 9.32),
            w("а", 9.76, 9.8),
            w("Кто", 10.12, 10.16),
            w("вы", 10.44, 10.6),
            w("такие?", 10.6, 10.64),
        ];
        let b = vec![
            w("Вы", 11.08, 11.24),
            w("ещё", 11.24, 11.28),
            w("на", 11.56, 11.72),
            w("связи?", 11.72, 11.92),
        ];
        t.on(Event::Commit(a), 12.6, 0.0, &mut o);
        t.on(Event::Commit(b), 12.6, 0.0, &mut o);
        t.on(part(&[]), 12.6, 0.0, &mut o);
        t.tick(12.6, 0.0, &mut o);
        let ends: Vec<_> = o.iter().filter(|m| m.event == "EndOfTurn").collect();
        assert_eq!(ends.len(), 1, "{:?}", events(&o));
        assert_eq!(
            (ends[0].transcript.as_str(), ends[0].trigger, ends[0].conf),
            (
                "Здравствуйте, а Кто вы такие? Вы ещё на связи?",
                Some("model"),
                0.675
            )
        );
        assert_eq!(t.count(), 1);
    }

    #[test]
    fn timeout_without_the_head_counts_from_the_newest_word() {
        let mut t = Turns::new(0.7, 1500, "ru");
        let mut o = vec![];
        t.on(part(&[w("алло", 0.1, 0.5)]), 0.8, 0.0, &mut o);
        t.tick(1.99, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        t.tick(2.0, 0.0, &mut o);
        let m = o.last().unwrap();
        assert_eq!(
            (m.event, m.trigger, m.silence),
            ("EndOfTurn", Some("timeout"), None)
        );
    }

    #[test]
    fn languages_follow_the_library() {
        let mut t = Turns::new(0.7, 5000, "ru");
        let mut o = vec![];
        t.on(Event::Language("uk".into()), 0.5, 0.0, &mut o);
        t.on(part(&[w("так", 0.1, 0.4)]), 0.6, 0.0, &mut o);
        let m = o.last().unwrap().json("r", "[]", 1, 0, 0);
        assert!(m.contains(r#""languages":["uk"]"#), "{m}");
        t.finish(1.0, 0.0, &mut o);
        let idle = o.last().unwrap().heartbeat().json("r", "[]", 9, 0, 0);
        assert!(
            idle.contains(r#""transcript":"","words":[],"languages":[],"languages_hinted":[]"#),
            "{idle}"
        );
    }
}

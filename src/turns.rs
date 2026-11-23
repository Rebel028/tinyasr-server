//! Turn tracking: library events in, Flux `TurnInfo` messages out. Pure logic, no I/O.

use tinyasr::{Event, Word};

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
}

impl Msg {
    pub fn json(&self, request_id: &str, seq: u64) -> String {
        let trigger = self
            .trigger
            .map_or(String::new(), |t| format!(",\"trigger\":\"{t}\""));
        format!(
            "{{\"type\":\"TurnInfo\",\"request_id\":\"{request_id}\",\"sequence_id\":{seq},\"event\":\"{}\",\"turn_index\":{},\"audio_window_start\":{:.2},\"audio_window_end\":{:.2},\"transcript\":{},\"words\":[],\"end_of_turn_confidence\":{:.3}{trigger}}}",
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
                ..*self
            }
        } else {
            Msg {
                event: "Update",
                trigger: None,
                transcript: self.transcript.clone(),
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
    last_p: f32,
    skip_commit: bool, // the turn was closed on open words: their later Commit is already reported
}

impl Turns {
    pub fn new(thr: f32, timeout_ms: u32) -> Turns {
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
            last_p: 0.0,
            skip_commit: false,
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
                self.last_p = 0.0;
                if self.partial.trim().is_empty() {
                    return;
                }
                self.words(&ws);
                self.begin(&ws, pos, off, out);
                out.push(self.msg("Update", pos, off, self.last_p, None));
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
                match self.pending {
                    Some((p, _)) => self.end(p, "model", pos, off, out),
                    None => out.push(self.msg("Update", pos, off, self.last_p, None)),
                }
            }
            Event::Eou { at, p } => {
                self.last_p = p;
                if self.open {
                    self.pending = (p >= self.thr).then_some((p, at as f64));
                }
            }
            Event::Language(_) => {}
        }
    }

    /// After each push: the silence timeout.
    pub fn tick(&mut self, pos: f64, off: f64, out: &mut Vec<Msg>) {
        if self.open && pos - self.last_word >= self.timeout {
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
        out.push(self.msg("EndOfTurn", pos, off, p, Some(trigger)));
        self.index += 1;
        self.open = false;
        self.committed.clear();
        self.partial.clear();
        self.pending = None;
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
        let mut t = Turns::new(0.7, 5000);
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
        let mut t = Turns::new(0.7, 5000);
        let mut o = vec![];
        t.on(part(&[w("один", 0.1, 0.5)]), 0.6, 0.0, &mut o);
        t.on(Event::Eou { at: 0.8, p: 0.6 }, 0.8, 0.0, &mut o);
        t.on(Event::Commit(vec![w("Один.", 0.1, 0.5)]), 1.0, 0.0, &mut o); // the library ends the sentence at 0.5
        t.on(Event::Eou { at: 1.6, p: 0.9 }, 1.6, 0.0, &mut o);
        t.on(part(&[w("два", 1.7, 2.0)]), 2.1, 0.0, &mut o); // speech after the pause cancels the yes
        t.on(Event::Commit(vec![w("Два.", 1.7, 2.0)]), 2.2, 0.0, &mut o); // ". and the next sentence started" rule
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        assert_eq!(o.last().unwrap().transcript, "Один. Два.");
        t.tick(6.9, 0.0, &mut o);
        assert!(o.iter().all(|m| m.event != "EndOfTurn"));
        t.tick(7.0, 0.0, &mut o);
        let m = o.last().unwrap();
        assert_eq!(
            (m.event, m.trigger, m.transcript.as_str()),
            ("EndOfTurn", Some("timeout"), "Один. Два.")
        );
    }

    #[test]
    fn timeout_on_open_words_skips_their_commit() {
        let mut t = Turns::new(0.7, 1000);
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
        };
        assert_eq!(
            m.json("r", 7),
            r#"{"type":"TurnInfo","request_id":"r","sequence_id":7,"event":"EndOfTurn","turn_index":2,"audio_window_start":1.00,"audio_window_end":2.50,"transcript":"a \"b\"","words":[],"end_of_turn_confidence":0.750,"trigger":"model"}"#
        );
    }
}

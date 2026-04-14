//! Tag-value encoding of FIX 4.4 messages and their framing on a byte stream.
//!
//! A message on the wire is a run of `tag=value` fields, each closed by the delimiter byte
//! [`SOH`]. It opens with the begin string (8) and the body length (9), and closes with the
//! checksum (10): the sum of every byte before the checksum field, modulo 256, as three digits.
//! The body length counts the bytes after the body length field up to the checksum field.
//!
//! [`encode`] writes the header in the order 8, 9, 35, 49, 56, 57, 50, 34, 52. [`decode`]
//! takes one whole message off the front of a buffer and accepts the header fields in any
//! order after the message type, as the server does not send them in the order it asks for.
//!
//! ```
//! use ctrader_connector::common::time::Timestamp;
//! use ctrader_connector::fix::codec::{Field, Frame, Header, decode, encode};
//!
//! let header = Header::new("0", "demo.broker.1", "CSERVER", 7, Timestamp::EPOCH);
//! let frame = Frame::new(header, vec![Field::new(112, "ping")]);
//! let mut bytes = encode(&frame)?;
//! assert_eq!(decode(&mut bytes)?, Some(frame));
//! assert!(bytes.is_empty());
//! # Ok::<(), ctrader_connector::common::errors::FixError>(())
//! ```

use std::fmt;
use std::fmt::Write as _;

use crate::common::errors::FixError;
use crate::common::time::Timestamp;

/// Byte that closes every field.
pub const SOH: u8 = 1;

/// Begin string of every message.
pub const BEGIN_STRING: &str = "FIX.4.4";

/// Longest body accepted or produced, in bytes.
pub const MAX_BODY_LENGTH: usize = 4 * 1024 * 1024;

const PREFIX: &[u8] = b"8=FIX.4.4\x019=";
const LENGTH_DIGITS: usize = 7;
const TRAILER_LENGTH: usize = 7;
const MILLIS_PER_DAY: i64 = 86_400_000;

const TAG_BEGIN_STRING: u32 = 8;
const TAG_BODY_LENGTH: u32 = 9;
const TAG_CHECKSUM: u32 = 10;
const TAG_MSG_TYPE: u32 = 35;
const TAG_SENDER_COMP_ID: u32 = 49;
const TAG_TARGET_COMP_ID: u32 = 56;
const TAG_TARGET_SUB_ID: u32 = 57;
const TAG_SENDER_SUB_ID: u32 = 50;
const TAG_SEQ: u32 = 34;
const TAG_SENDING_TIME: u32 = 52;
const TAG_POSS_DUP: u32 = 43;
const TAG_ORIG_SENDING_TIME: u32 = 122;
const TAG_PASSWORD: u32 = 554;

/// One `tag=value` field.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Field {
    /// Number of the tag.
    pub tag: u32,
    /// Value as the wire carries it.
    pub value: String,
}

impl Field {
    /// Builds a field.
    pub fn new(tag: u32, value: impl Into<String>) -> Self {
        Self {
            tag,
            value: value.into(),
        }
    }
}

impl fmt::Debug for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.tag == TAG_PASSWORD {
            write!(f, "{}=***", self.tag)
        } else {
            write!(f, "{}={}", self.tag, self.value)
        }
    }
}

/// Standard header of a message, without the begin string and the body length.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Header {
    /// Type of the message (35).
    pub msg_type: String,
    /// Identifier of the sender (49).
    pub sender_comp_id: String,
    /// Identifier of the receiver (56).
    pub target_comp_id: String,
    /// Session qualifier of the receiver (57).
    pub target_sub_id: Option<String>,
    /// Session qualifier of the sender (50).
    pub sender_sub_id: Option<String>,
    /// Sequence number of the message (34).
    pub seq: u64,
    /// Time the message was sent (52).
    pub sending_time: Timestamp,
    /// Tells whether the message repeats one sent before under the same sequence number (43).
    pub poss_dup: bool,
    /// Time the repeated message was sent first (122).
    pub orig_sending_time: Option<Timestamp>,
}

impl Header {
    /// Builds a header without session qualifiers that marks no repetition.
    pub fn new(
        msg_type: impl Into<String>,
        sender_comp_id: impl Into<String>,
        target_comp_id: impl Into<String>,
        seq: u64,
        sending_time: Timestamp,
    ) -> Self {
        Self {
            msg_type: msg_type.into(),
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
            target_sub_id: None,
            sender_sub_id: None,
            seq,
            sending_time,
            poss_dup: false,
            orig_sending_time: None,
        }
    }

    /// Sets both session qualifiers.
    #[must_use]
    pub fn qualified(mut self, target_sub_id: &str, sender_sub_id: &str) -> Self {
        self.target_sub_id = Some(target_sub_id.to_owned());
        self.sender_sub_id = Some(sender_sub_id.to_owned());
        self
    }
}

/// One message: its header and the fields of its body in the order of the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Frame {
    /// Standard header.
    pub header: Header,
    /// Fields of the body.
    pub body: Vec<Field>,
}

impl Frame {
    /// Builds a message.
    #[must_use]
    pub const fn new(header: Header, body: Vec<Field>) -> Self {
        Self { header, body }
    }
}

/// Computes the checksum of the bytes: their sum modulo 256.
#[must_use]
pub fn checksum(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0_u8, |sum, byte| sum.wrapping_add(*byte))
}

fn put(out: &mut String, msg_type: &str, tag: u32, value: &str) -> Result<(), FixError> {
    let plain = !value.is_empty() && !value.bytes().any(|byte| byte == SOH);
    let framing = matches!(tag, 0 | TAG_BEGIN_STRING | TAG_BODY_LENGTH | TAG_CHECKSUM);
    if !plain || framing {
        return Err(FixError::InvalidField {
            msg_type: msg_type.to_owned(),
            tag,
        });
    }
    let _ = write!(out, "{tag}={value}\u{1}");
    Ok(())
}

/// Encodes a message as the bytes of the wire.
///
/// # Errors
///
/// Returns [`FixError::InvalidField`] when a value is empty or holds the delimiter, or a body
/// field carries a tag of the framing, and [`FixError::TooLong`] when the body exceeds
/// [`MAX_BODY_LENGTH`].
pub fn encode(frame: &Frame) -> Result<Vec<u8>, FixError> {
    let header = &frame.header;
    let kind = header.msg_type.as_str();
    let mut body = String::new();
    put(&mut body, kind, TAG_MSG_TYPE, kind)?;
    put(&mut body, kind, TAG_SENDER_COMP_ID, &header.sender_comp_id)?;
    put(&mut body, kind, TAG_TARGET_COMP_ID, &header.target_comp_id)?;
    if let Some(qualifier) = &header.target_sub_id {
        put(&mut body, kind, TAG_TARGET_SUB_ID, qualifier)?;
    }
    if let Some(qualifier) = &header.sender_sub_id {
        put(&mut body, kind, TAG_SENDER_SUB_ID, qualifier)?;
    }
    put(&mut body, kind, TAG_SEQ, &header.seq.to_string())?;
    let sent = format_time(header.sending_time, true);
    put(&mut body, kind, TAG_SENDING_TIME, &sent)?;
    if header.poss_dup {
        put(&mut body, kind, TAG_POSS_DUP, "Y")?;
    }
    if let Some(first) = header.orig_sending_time {
        let first = format_time(first, true);
        put(&mut body, kind, TAG_ORIG_SENDING_TIME, &first)?;
    }
    for field in &frame.body {
        put(&mut body, kind, field.tag, &field.value)?;
    }
    if body.len() > MAX_BODY_LENGTH {
        return Err(FixError::TooLong {
            length: body.len(),
            limit: MAX_BODY_LENGTH,
        });
    }
    let mut out = format!("8={BEGIN_STRING}\u{1}9={}\u{1}", body.len()).into_bytes();
    out.extend_from_slice(body.as_bytes());
    let sum = checksum(&out);
    out.extend_from_slice(format!("10={sum:03}\u{1}").as_bytes());
    Ok(out)
}

fn body_length(digits: &[u8]) -> Result<usize, FixError> {
    let numeric = !digits.is_empty()
        && digits.len() <= LENGTH_DIGITS
        && digits.iter().all(u8::is_ascii_digit);
    if !numeric {
        return Err(FixError::Garbled("the body length is not a number"));
    }
    let length = digits.iter().fold(0_usize, |length, digit| {
        length
            .saturating_mul(10)
            .saturating_add(usize::from(digit.saturating_sub(b'0')))
    });
    if length > MAX_BODY_LENGTH {
        return Err(FixError::TooLong {
            length,
            limit: MAX_BODY_LENGTH,
        });
    }
    Ok(length)
}

fn found_checksum(trailer: &[u8]) -> Option<u8> {
    let digits = trailer.strip_prefix(b"10=")?.strip_suffix(&[SOH])?;
    if digits.len() != 3 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = digits.iter().fold(0_u16, |value, digit| {
        value
            .saturating_mul(10)
            .saturating_add(u16::from(digit.saturating_sub(b'0')))
    });
    u8::try_from(value).ok()
}

fn fields(body: &[u8]) -> Result<Vec<Field>, FixError> {
    let Some(inner) = body.strip_suffix(&[SOH]) else {
        return Err(FixError::Garbled(
            "the body does not end with the delimiter",
        ));
    };
    inner
        .split(|byte| *byte == SOH)
        .map(|piece| {
            let text = std::str::from_utf8(piece)
                .map_err(|_| FixError::Garbled("a field is not valid UTF-8"))?;
            let (tag, value) = text
                .split_once('=')
                .ok_or(FixError::Garbled("a field has no value"))?;
            let numeric =
                !tag.is_empty() && tag.len() <= 9 && tag.bytes().all(|b| b.is_ascii_digit());
            let tag = tag
                .parse::<u32>()
                .ok()
                .filter(|tag| numeric && *tag > 0)
                .ok_or(FixError::Garbled("a tag is not a positive number"))?;
            if value.is_empty() {
                return Err(FixError::Garbled("a field has an empty value"));
            }
            Ok(Field::new(tag, value))
        })
        .collect()
}

fn header_of(all: Vec<Field>) -> Result<Frame, FixError> {
    let mut fields = all.into_iter();
    let msg_type = match fields.next() {
        Some(field) if field.tag == TAG_MSG_TYPE => field.value,
        _ => return Err(FixError::Garbled("the message type is not the third field")),
    };
    let mut slots: [(u32, Option<String>); 8] = [
        (TAG_SENDER_COMP_ID, None),
        (TAG_TARGET_COMP_ID, None),
        (TAG_TARGET_SUB_ID, None),
        (TAG_SENDER_SUB_ID, None),
        (TAG_SEQ, None),
        (TAG_SENDING_TIME, None),
        (TAG_POSS_DUP, None),
        (TAG_ORIG_SENDING_TIME, None),
    ];
    let mut body = Vec::new();
    for field in fields {
        match slots.iter_mut().find(|(tag, _)| *tag == field.tag) {
            Some((_, slot)) if slot.is_none() => *slot = Some(field.value),
            Some(_) => {
                return Err(FixError::InvalidField {
                    msg_type,
                    tag: field.tag,
                });
            }
            None => body.push(field),
        }
    }
    let [
        sender,
        target,
        target_sub,
        sender_sub,
        seq,
        sent,
        poss_dup,
        first,
    ] = slots;
    let missing = |tag: u32| FixError::MissingField {
        msg_type: msg_type.clone(),
        tag,
    };
    let invalid = |tag: u32| FixError::InvalidField {
        msg_type: msg_type.clone(),
        tag,
    };
    let seq = seq
        .1
        .ok_or_else(|| missing(TAG_SEQ))?
        .parse::<u64>()
        .map_err(|_| invalid(TAG_SEQ))?;
    let sending_time = sent
        .1
        .ok_or_else(|| missing(TAG_SENDING_TIME))
        .and_then(|text| parse_time(&text).ok_or_else(|| invalid(TAG_SENDING_TIME)))?;
    let orig_sending_time = first
        .1
        .map(|text| parse_time(&text).ok_or_else(|| invalid(TAG_ORIG_SENDING_TIME)))
        .transpose()?;
    let poss_dup = match poss_dup.1.as_deref() {
        None | Some("N") => false,
        Some("Y") => true,
        Some(_) => return Err(invalid(TAG_POSS_DUP)),
    };
    let header = Header {
        sender_comp_id: sender.1.ok_or_else(|| missing(TAG_SENDER_COMP_ID))?,
        target_comp_id: target.1.ok_or_else(|| missing(TAG_TARGET_COMP_ID))?,
        target_sub_id: target_sub.1,
        sender_sub_id: sender_sub.1,
        seq,
        sending_time,
        poss_dup,
        orig_sending_time,
        msg_type,
    };
    Ok(Frame { header, body })
}

/// Takes one whole message off the front of `buffer` and decodes it.
///
/// Returns `Ok(None)` and leaves the buffer untouched while the message is incomplete. A
/// message whose length is known is removed from the buffer even when it is rejected, so the
/// next call reads the message after it. [`FixError::Garbled`] for bytes that do not open a
/// message and [`FixError::TooLong`] leave the buffer as it is: the stream has lost its
/// framing and must be abandoned.
///
/// # Errors
///
/// Returns [`FixError::Garbled`] when the bytes are not a message, [`FixError::TooLong`] when
/// the announced body exceeds [`MAX_BODY_LENGTH`], [`FixError::Checksum`] when the checksum
/// does not match, and [`FixError::MissingField`] or [`FixError::InvalidField`] when the
/// header is incomplete or holds a value of the wrong form.
pub fn decode(buffer: &mut Vec<u8>) -> Result<Option<Frame>, FixError> {
    match take(buffer)? {
        None => Ok(None),
        Some(Ok(frame)) => Ok(Some(frame)),
        Some(Err(unreadable)) => Err(unreadable.error),
    }
}

/// A whole message that was taken off the stream and cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Unreadable {
    /// Sequence number of the message, when its bytes are intact and carry exactly one.
    pub(crate) seq: Option<u64>,
    /// Why the message cannot be read.
    pub(crate) error: FixError,
}

fn scanned_seq(body: &[u8]) -> Option<u64> {
    let mut numbers = body
        .split(|byte| *byte == SOH)
        .filter_map(|piece| piece.strip_prefix(b"34="));
    let digits = numbers.next()?;
    if numbers.next().is_some() {
        return None;
    }
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// Takes one whole message off the front of `buffer`, as [`decode`] does, and keeps apart
/// the message that was removed but cannot be read from the stream that lost its framing.
pub(crate) fn take(buffer: &mut Vec<u8>) -> Result<Option<Result<Frame, Unreadable>>, FixError> {
    let common = buffer.len().min(PREFIX.len());
    if buffer.get(..common) != PREFIX.get(..common) {
        return Err(FixError::Garbled("the bytes do not open a message"));
    }
    let Some(rest) = buffer.get(PREFIX.len()..) else {
        return Ok(None);
    };
    let Some(end) = rest.iter().position(|byte| *byte == SOH) else {
        return body_length(
            rest.get(..rest.len().min(LENGTH_DIGITS + 1))
                .unwrap_or(rest),
        )
        .map(|_| None)
        .or_else(|error| {
            if rest.is_empty() {
                Ok(None)
            } else {
                Err(error)
            }
        });
    };
    let length = body_length(rest.get(..end).unwrap_or(rest))?;
    let start = PREFIX.len().saturating_add(end).saturating_add(1);
    let trailer = start.saturating_add(length);
    let total = trailer.saturating_add(TRAILER_LENGTH);
    if buffer.len() < total {
        return Ok(None);
    }
    let message: Vec<u8> = buffer.drain(..total).collect();
    let (counted, tail) = message.split_at_checked(trailer).unwrap_or((&message, &[]));
    let damaged = |error| Ok(Some(Err(Unreadable { seq: None, error })));
    let Some(found) = found_checksum(tail) else {
        return damaged(FixError::Garbled(
            "the checksum field does not close the message",
        ));
    };
    let computed = checksum(counted);
    if computed != found {
        return damaged(FixError::Checksum { computed, found });
    }
    let body = counted.get(start..).unwrap_or(&[]);
    Ok(Some(fields(body).and_then(header_of).map_err(|error| {
        Unreadable {
            seq: scanned_seq(body),
            error,
        }
    })))
}

fn civil(days: i64) -> (i64, i64, i64) {
    let shifted = days.saturating_add(719_468);
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era.saturating_add(era.saturating_mul(400));
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn days_before(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Writes a moment as the UTC timestamp of the wire, `YYYYMMDD-HH:MM:SS`, followed by the
/// milliseconds when `millis` is set.
#[must_use]
pub fn format_time(moment: Timestamp, millis: bool) -> String {
    let total = moment.as_millis();
    let (year, month, day) = civil(total.div_euclid(MILLIS_PER_DAY));
    let rest = total.rem_euclid(MILLIS_PER_DAY);
    let (hour, minute, second) = (rest / 3_600_000, rest / 60_000 % 60, rest / 1_000 % 60);
    let mut text = format!("{year:04}{month:02}{day:02}-{hour:02}:{minute:02}:{second:02}");
    if millis {
        let _ = write!(text, ".{:03}", rest % 1_000);
    }
    text
}

fn number(text: &str, from: usize, to: usize) -> Option<i64> {
    let digits = text.get(from..to)?;
    if digits.bytes().all(|byte| byte.is_ascii_digit()) {
        digits.parse().ok()
    } else {
        None
    }
}

/// Reads a UTC timestamp of the wire, `YYYYMMDD-HH:MM:SS` with or without milliseconds.
///
/// Returns `None` when the text has another form or names a moment that does not exist.
#[must_use]
pub fn parse_time(text: &str) -> Option<Timestamp> {
    let millis = match text.len() {
        17 => 0,
        21 if text.get(17..18) == Some(".") => number(text, 18, 21)?,
        _ => return None,
    };
    let punctuated = text.get(8..9) == Some("-")
        && text.get(11..12) == Some(":")
        && text.get(14..15) == Some(":");
    if !punctuated {
        return None;
    }
    let (year, month, day) = (
        number(text, 0, 4)?,
        number(text, 4, 6)?,
        number(text, 6, 8)?,
    );
    let (hour, minute, second) = (
        number(text, 9, 11)?,
        number(text, 12, 14)?,
        number(text, 15, 17)?,
    );
    let days = days_before(year, month, day);
    let exists = (1..=12).contains(&month) && civil(days) == (year, month, day);
    if !exists || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let of_day = hour * 3_600_000 + minute * 60_000 + second * 1_000 + millis;
    Timestamp::from_millis(days.checked_mul(MILLIS_PER_DAY)?.checked_add(of_day)?).ok()
}

#[cfg(test)]
mod tests {
    use super::{
        BEGIN_STRING, Field, Frame, Header, MAX_BODY_LENGTH, SOH, checksum, decode, encode,
        format_time, parse_time, take,
    };
    use crate::common::errors::FixError;
    use crate::common::time::Timestamp;

    fn wire(text: &str) -> Vec<u8> {
        text.replace('|', "\u{1}").into_bytes()
    }

    fn sealed(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\u{1}");
        let mut out = format!("8={BEGIN_STRING}\u{1}9={}\u{1}{body}", body.len()).into_bytes();
        let sum = checksum(&out);
        out.extend_from_slice(format!("10={sum:03}\u{1}").as_bytes());
        out
    }

    fn moment(millis: i64) -> Timestamp {
        Timestamp::from_millis(millis).expect("moment")
    }

    fn heartbeat(seq: u64) -> Frame {
        let header = Header::new(
            "0",
            "demo.broker.1",
            "CSERVER",
            seq,
            moment(1_484_640_184_155),
        )
        .qualified("QUOTE", "QUOTE");
        Frame::new(header, vec![Field::new(112, "ping")])
    }

    #[test]
    fn header_is_written_in_the_order_the_server_asks_for() {
        let bytes = encode(&heartbeat(7)).expect("encode");
        let text = String::from_utf8(bytes)
            .expect("text")
            .replace('\u{1}', "|");
        assert_eq!(
            text,
            "8=FIX.4.4|9=90|35=0|49=demo.broker.1|56=CSERVER|57=QUOTE|50=QUOTE|34=7|52=20170117-08:03:04.155|112=ping|10=146|"
        );
    }

    #[test]
    fn body_length_and_checksum_match_values_computed_independently() {
        let example = wire(
            "8=FIX.4.4|9=111|35=A|34=1|49=CSERVER|50=TRADE|52=20170117-08:03:04.509|56=live.theBroker.12345|57=any_string|98=0|108=30|141=Y|10=028|",
        );
        let mut buffer = example.clone();
        let frame = decode(&mut buffer).expect("decode").expect("whole message");
        assert!(buffer.is_empty());
        assert_eq!(frame.header.msg_type, "A");
        assert_eq!(frame.header.seq, 1);
        assert_eq!(frame.header.sender_comp_id, "CSERVER");
        assert_eq!(frame.header.sender_sub_id.as_deref(), Some("TRADE"));
        assert_eq!(frame.header.target_sub_id.as_deref(), Some("any_string"));
        assert_eq!(
            format_time(frame.header.sending_time, true),
            "20170117-08:03:04.509"
        );
        assert_eq!(
            frame.body,
            vec![
                Field::new(98, "0"),
                Field::new(108, "30"),
                Field::new(141, "Y")
            ]
        );
        let body = example
            .get(..example.len() - 7)
            .expect("message without the trailer");
        assert_eq!(checksum(body), 28);
    }

    #[test]
    fn message_round_trips_with_the_marks_of_a_repetition() {
        let mut frame = heartbeat(12);
        frame.header.poss_dup = true;
        frame.header.orig_sending_time = Some(moment(1_484_640_000_000));
        let mut bytes = encode(&frame).expect("encode");
        assert_eq!(decode(&mut bytes).expect("decode"), Some(frame));
    }

    #[test]
    fn messages_are_framed_across_partial_reads_and_several_per_read() {
        let first = encode(&heartbeat(1)).expect("first");
        let second = encode(&heartbeat(2)).expect("second");
        let mut stream = first.clone();
        stream.extend_from_slice(&second);
        let mut buffer = Vec::new();
        let mut decoded = Vec::new();
        for byte in &stream {
            buffer.push(*byte);
            while let Some(frame) = decode(&mut buffer).expect("decode") {
                decoded.push(frame.header.seq);
            }
        }
        assert_eq!(decoded, vec![1, 2]);
        let mut both = stream.clone();
        assert_eq!(
            decode(&mut both).expect("first").map(|f| f.header.seq),
            Some(1)
        );
        assert_eq!(
            decode(&mut both).expect("second").map(|f| f.header.seq),
            Some(2)
        );
        assert_eq!(decode(&mut both).expect("empty"), None);
    }

    #[test]
    fn bad_checksum_is_rejected_and_the_next_message_is_still_read() {
        let mut bytes = encode(&heartbeat(1)).expect("encode");
        let at = bytes.len() - 2;
        let digit = bytes.get_mut(at).expect("last digit of the checksum");
        *digit = if *digit == b'9' { b'0' } else { *digit + 1 };
        bytes.extend_from_slice(&encode(&heartbeat(2)).expect("next"));
        assert!(matches!(decode(&mut bytes), Err(FixError::Checksum { .. })));
        assert_eq!(
            decode(&mut bytes).expect("next").map(|f| f.header.seq),
            Some(2)
        );
    }

    #[test]
    fn unreadable_message_keeps_its_number_only_when_its_bytes_are_intact() {
        let seq = |bytes: &mut Vec<u8>| match take(bytes) {
            Ok(Some(Err(unreadable))) => Ok(unreadable.seq),
            other => Err(other),
        };
        assert_eq!(seq(&mut sealed("35=0|49=A|56=B|34=7|")), Ok(Some(7)));
        assert_eq!(seq(&mut sealed("35=0|49=A|56=B|34=7|58=|")), Ok(Some(7)));
        assert_eq!(seq(&mut sealed("35=0|49=A|56=B|34=7|34=8|")), Ok(None));
        assert_eq!(seq(&mut sealed("35=0|49=A|56=B|34=x|")), Ok(None));
        assert_eq!(seq(&mut sealed("35=0|49=A|56=B|")), Ok(None));
        let mut damaged = sealed("35=0|49=A|56=B|34=7|");
        let at = damaged.len() - 2;
        let digit = damaged.get_mut(at).expect("last digit of the checksum");
        *digit = if *digit == b'9' { b'0' } else { *digit + 1 };
        assert_eq!(seq(&mut damaged), Ok(None));
    }

    #[test]
    fn bad_body_length_is_rejected() {
        let good = String::from_utf8(encode(&heartbeat(1)).expect("encode")).expect("text");
        let mut short = good.replace("9=90", "9=89").into_bytes();
        assert!(matches!(decode(&mut short), Err(FixError::Garbled(_))));
        let mut long = good.replace("9=90", "9=91").into_bytes();
        assert_eq!(decode(&mut long).expect("incomplete"), None);
        long.push(b'8');
        assert!(decode(&mut long).is_err());
        let mut huge = wire("8=FIX.4.4|9=9999999|35=0|");
        assert_eq!(
            decode(&mut huge),
            Err(FixError::TooLong {
                length: 9_999_999,
                limit: MAX_BODY_LENGTH
            })
        );
        let mut endless = wire("8=FIX.4.4|9=12345678");
        assert!(matches!(decode(&mut endless), Err(FixError::Garbled(_))));
        let mut worded = wire("8=FIX.4.4|9=1x|");
        assert!(matches!(decode(&mut worded), Err(FixError::Garbled(_))));
    }

    #[test]
    fn incomplete_openings_wait_for_more_bytes() {
        for opening in [
            "",
            "8",
            "8=FIX.4.4",
            "8=FIX.4.4|9=",
            "8=FIX.4.4|9=1",
            "8=FIX.4.4|9=10|35",
        ] {
            let mut buffer = wire(opening);
            assert_eq!(decode(&mut buffer), Ok(None), "{opening}");
            assert_eq!(buffer, wire(opening));
        }
    }

    #[test]
    fn header_fields_are_required_and_typed() {
        let cases = [
            ("49=A|56=B|34=1|52=20170117-08:03:04|", "the message type"),
            ("35=0|56=B|34=1|52=20170117-08:03:04|", "sender"),
            ("35=0|49=A|34=1|52=20170117-08:03:04|", "target"),
            ("35=0|49=A|56=B|52=20170117-08:03:04|", "sequence number"),
            ("35=0|49=A|56=B|34=1|", "sending time"),
            (
                "35=0|49=A|56=B|34=x|52=20170117-08:03:04|",
                "sequence number form",
            ),
            ("35=0|49=A|56=B|34=1|52=2017|", "sending time form"),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|43=X|",
                "repetition mark",
            ),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|122=x|",
                "first sending time",
            ),
            (
                "35=0|49=A|49=A|56=B|34=1|52=20170117-08:03:04|",
                "repeated field",
            ),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|112|",
                "field without value",
            ),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|112=|",
                "empty value",
            ),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|x=1|",
                "worded tag",
            ),
            ("35=0|49=A|56=B|34=1|52=20170117-08:03:04|0=1|", "tag zero"),
            (
                "35=0|49=A|56=B|34=1|52=20170117-08:03:04|112=a",
                "no closing delimiter",
            ),
        ];
        for (body, what) in cases {
            let mut buffer = sealed(body);
            assert!(decode(&mut buffer).is_err(), "{what}");
            assert!(buffer.is_empty(), "{what}");
        }
        let mut plain = sealed("35=0|49=A|56=B|34=1|52=20170117-08:03:04|43=N|");
        let frame = decode(&mut plain).expect("decode").expect("message");
        assert!(!frame.header.poss_dup);
        assert_eq!(frame.header.target_sub_id, None);
    }

    #[test]
    fn values_that_cannot_be_framed_are_refused_before_encoding() {
        let refused = |field: Field| {
            let mut frame = heartbeat(1);
            frame.body = vec![field];
            encode(&frame)
        };
        for field in [
            Field::new(112, ""),
            Field::new(112, "a\u{1}b"),
            Field::new(8, "FIX.4.4"),
            Field::new(9, "5"),
            Field::new(10, "000"),
            Field::new(0, "x"),
        ] {
            assert_eq!(
                refused(field.clone()),
                Err(FixError::InvalidField {
                    msg_type: "0".to_owned(),
                    tag: field.tag
                })
            );
        }
        let long = Field::new(58, "x".repeat(MAX_BODY_LENGTH));
        assert!(matches!(refused(long), Err(FixError::TooLong { .. })));
        let mut unnamed = heartbeat(1);
        unnamed.header.sender_comp_id = String::new();
        assert!(encode(&unnamed).is_err());
    }

    #[test]
    fn password_never_appears_in_debug_output() {
        let field = Field::new(554, "hunter2");
        assert_eq!(format!("{field:?}"), "554=***");
        assert_eq!(format!("{:?}", Field::new(553, "12345")), "553=12345");
    }

    #[test]
    fn malformed_input_never_panics() {
        let good = encode(&heartbeat(3)).expect("encode");
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let interesting = [SOH, b'=', b'8', b'9', b'1', b'0', b'|', 0, 255, b'F'];
        for round in 0..4_000 {
            let mut bytes = good.clone();
            let edits = 1 + next() % 4;
            for _ in 0..edits {
                let at = usize::try_from(next()).unwrap_or(0) % bytes.len();
                let pick = usize::try_from(next()).unwrap_or(0) % interesting.len();
                let byte = interesting.get(pick).copied().unwrap_or(0);
                match next() % 3 {
                    0 => {
                        if let Some(slot) = bytes.get_mut(at) {
                            *slot = byte;
                        }
                    }
                    1 => bytes.insert(at, byte),
                    _ => {
                        bytes.remove(at);
                    }
                }
                if bytes.is_empty() {
                    bytes.push(byte);
                }
            }
            if round % 2 == 0 {
                bytes.extend_from_slice(&good);
            }
            for _ in 0..8 {
                let before = bytes.len();
                match decode(&mut bytes) {
                    Ok(None) => break,
                    Err(_) if bytes.len() == before => break,
                    Ok(Some(_)) | Err(_) => {}
                }
            }
        }
        for length in 0..good.len() {
            let mut cut = good.get(..length).expect("prefix").to_vec();
            assert_eq!(decode(&mut cut), Ok(None), "prefix of {length} bytes");
        }
    }

    #[test]
    fn timestamps_are_written_and_read_as_utc() {
        let cases = [
            (0, "19700101-00:00:00.000"),
            (951_782_400_000, "20000229-00:00:00.000"),
            (1_484_640_184_155, "20170117-08:03:04.155"),
            (4_102_444_799_999, "20991231-23:59:59.999"),
        ];
        for (millis, text) in cases {
            assert_eq!(format_time(moment(millis), true), text);
            assert_eq!(parse_time(text), Some(moment(millis)));
        }
        assert_eq!(
            format_time(moment(1_484_640_184_155), false),
            "20170117-08:03:04"
        );
        assert_eq!(
            parse_time("20170117-08:03:04"),
            Some(moment(1_484_640_184_000))
        );
        for text in [
            "",
            "20170117",
            "20170117 08:03:04",
            "20170117-08-03-04",
            "20170117-08:03:04.1",
            "20170117-08:03:04,155",
            "20170117-08:03:04.1x5",
            "20170230-08:03:04",
            "20171301-08:03:04",
            "20170100-08:03:04",
            "20170117-24:03:04",
            "20170117-08:60:04",
            "20170117-08:03:60",
            "2017011x-08:03:04",
            "19691231-23:59:59",
            "+0170117-08:03:04",
        ] {
            assert_eq!(parse_time(text), None, "{text}");
        }
    }
}

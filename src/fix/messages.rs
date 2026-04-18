//! Typed messages of the FIX 4.4 sessions and their conversion to and from fields.
//!
//! Every message the library sends or understands has a struct here and a variant in
//! [`Message`]. A message is parsed whole: a required field that is absent or a value of the
//! wrong form rejects the message with a typed error instead of yielding a partial value.
//! Prices and quantities are decimals, symbols, orders and positions are identified by their
//! typed ids.
//!
//! ```
//! use ctrader_connector::common::time::Timestamp;
//! use ctrader_connector::fix::codec::{Header, decode, encode};
//! use ctrader_connector::fix::messages::{Message, TestRequest};
//!
//! let message = Message::from(TestRequest {
//!     test_req_id: "ping".to_owned(),
//! });
//! let header = Header::new("", "demo.broker.1", "CSERVER", 2, Timestamp::EPOCH);
//! let mut bytes = encode(&message.to_frame(header))?;
//! let frame = decode(&mut bytes)?.expect("whole message");
//! assert_eq!(Message::from_frame(&frame)?, message);
//! # Ok::<(), ctrader_connector::common::errors::FixError>(())
//! ```

use std::str::FromStr;

use rust_decimal::Decimal;

use crate::common::config::FixPassword;
use crate::common::decimals::{Price, Volume};
use crate::common::errors::FixError;
use crate::common::ids::{OrderId, PositionId, SymbolId};
use crate::common::time::Timestamp;
use crate::fix::codec::{Field, Frame, Header, format_time, parse_time};

trait Wire: Sized {
    fn to_wire(&self) -> String;
    fn from_wire(text: &str) -> Option<Self>;
}

impl Wire for String {
    fn to_wire(&self) -> String {
        self.clone()
    }

    fn from_wire(text: &str) -> Option<Self> {
        Some(text.to_owned())
    }
}

impl Wire for bool {
    fn to_wire(&self) -> String {
        if *self { "Y" } else { "N" }.to_owned()
    }

    fn from_wire(text: &str) -> Option<Self> {
        match text {
            "Y" => Some(true),
            "N" => Some(false),
            _ => None,
        }
    }
}

impl Wire for Timestamp {
    fn to_wire(&self) -> String {
        format_time(*self, self.as_millis() % 1_000 != 0)
    }

    fn from_wire(text: &str) -> Option<Self> {
        parse_time(text)
    }
}

impl Wire for FixPassword {
    fn to_wire(&self) -> String {
        self.expose().to_owned()
    }

    fn from_wire(text: &str) -> Option<Self> {
        Self::new(text).ok()
    }
}

macro_rules! wire_from_str {
    ($($ty:ty),*) => {
        $(impl Wire for $ty {
            fn to_wire(&self) -> String {
                self.to_string()
            }

            fn from_wire(text: &str) -> Option<Self> {
                <$ty>::from_str(text).ok()
            }
        })*
    };
}

wire_from_str!(
    u32, u64, Decimal, Price, Volume, SymbolId, OrderId, PositionId
);

macro_rules! wire_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident => $wire:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vdoc])* $variant,)*
        }

        impl $name {
            /// Returns the value the wire carries.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)*
                }
            }
        }

        impl Wire for $name {
            fn to_wire(&self) -> String {
                self.as_str().to_owned()
            }

            fn from_wire(text: &str) -> Option<Self> {
                match text {
                    $($wire => Some(Self::$variant),)*
                    _ => None,
                }
            }
        }
    };
}

wire_enum! {
    /// Side of an order (54).
    Side {
        /// Buy.
        Buy => "1",
        /// Sell.
        Sell => "2",
    }
}

wire_enum! {
    /// Type of an order (40).
    OrderKind {
        /// Executed at once at the price of the market.
        Market => "1",
        /// Executed at the limit price or better.
        Limit => "2",
        /// Becomes a market order once the stop price is reached.
        Stop => "3",
        /// Becomes a limit order once the stop price is reached. The server offers no such
        /// order over FIX: the kind is read from a report and no typed request sends it.
        StopLimit => "4",
    }
}

wire_enum! {
    /// How long an order stays in force (59).
    TimeInForce {
        /// Until it is cancelled.
        GoodTillCancel => "1",
        /// Filled at once as far as possible, the rest is cancelled.
        ImmediateOrCancel => "3",
        /// Until the expiry time.
        GoodTillDate => "6",
    }
}

wire_enum! {
    /// What an execution report tells (150).
    ExecType {
        /// The order was accepted.
        New => "0",
        /// The order was cancelled.
        Canceled => "4",
        /// The order was replaced.
        Replaced => "5",
        /// The order was rejected.
        Rejected => "8",
        /// The order expired.
        Expired => "C",
        /// The order was filled, wholly or in part.
        Trade => "F",
        /// The report answers a status request.
        OrderStatus => "I",
    }
}

wire_enum! {
    /// State of an order (39).
    OrderStatus {
        /// Accepted and not filled.
        New => "0",
        /// Filled in part.
        PartiallyFilled => "1",
        /// Filled.
        Filled => "2",
        /// Cancelled.
        Canceled => "4",
        /// Replaced.
        Replaced => "5",
        /// Rejected.
        Rejected => "8",
        /// Expired.
        Expired => "C",
    }
}

wire_enum! {
    /// Request a cancel reject answers (434).
    CancelRejectResponse {
        /// An order cancel request.
        Cancel => "1",
        /// An order cancel/replace request.
        Replace => "2",
    }
}

wire_enum! {
    /// Depth of the market data asked for (264).
    MarketDepth {
        /// The full book.
        Full => "0",
        /// The best bid and the best offer only.
        Spot => "1",
    }
}

wire_enum! {
    /// Side of a market data entry (269).
    BookSide {
        /// Bid.
        Bid => "0",
        /// Offer.
        Offer => "1",
    }
}

struct Reader<'a> {
    kind: &'static str,
    fields: &'a [Field],
}

impl<'a> Reader<'a> {
    const fn new(kind: &'static str, fields: &'a [Field]) -> Self {
        Self { kind, fields }
    }

    fn missing(&self, tag: u32) -> FixError {
        FixError::MissingField {
            msg_type: self.kind.to_owned(),
            tag,
        }
    }

    fn invalid(&self, tag: u32) -> FixError {
        FixError::InvalidField {
            msg_type: self.kind.to_owned(),
            tag,
        }
    }

    fn text(&self, tag: u32) -> Option<&'a str> {
        self.fields
            .iter()
            .find(|field| field.tag == tag)
            .map(|field| field.value.as_str())
    }

    fn optional<T: Wire>(&self, tag: u32) -> Result<Option<T>, FixError> {
        self.text(tag)
            .map(|text| T::from_wire(text).ok_or_else(|| self.invalid(tag)))
            .transpose()
    }

    fn required<T: Wire>(&self, tag: u32) -> Result<T, FixError> {
        self.optional(tag)?.ok_or_else(|| self.missing(tag))
    }

    fn lax<T: Wire>(&self, tag: u32) -> Option<T> {
        self.text(tag).and_then(T::from_wire)
    }

    fn group(&self, count_tag: u32, members: &[u32]) -> Result<Vec<Self>, FixError> {
        let Some(at) = self.fields.iter().position(|field| field.tag == count_tag) else {
            return Ok(Vec::new());
        };
        let count: u32 = self.required(count_tag)?;
        let rest = self.fields.get(at.saturating_add(1)..).unwrap_or(&[]);
        let length = rest
            .iter()
            .position(|field| !members.contains(&field.tag))
            .unwrap_or(rest.len());
        let inside = rest.get(..length).unwrap_or(rest);
        let delimiter = members.first().copied().unwrap_or(0);
        let mut entries = Vec::new();
        let mut start = 0_usize;
        for (index, field) in inside.iter().enumerate() {
            if field.tag == delimiter && index > 0 {
                entries.push(Self::new(
                    self.kind,
                    inside.get(start..index).unwrap_or(&[]),
                ));
                start = index;
            }
        }
        match inside.first() {
            Some(first) if first.tag == delimiter => {
                entries.push(Self::new(self.kind, inside.get(start..).unwrap_or(&[])));
            }
            Some(_) => return Err(self.invalid(count_tag)),
            None => {}
        }
        if u32::try_from(entries.len()).ok() == Some(count) {
            Ok(entries)
        } else {
            Err(self.invalid(count_tag))
        }
    }
}

fn put<T: Wire>(out: &mut Vec<Field>, tag: u32, value: &T) {
    out.push(Field::new(tag, value.to_wire()));
}

fn put_optional<T: Wire>(out: &mut Vec<Field>, tag: u32, value: Option<&T>) {
    if let Some(value) = value {
        put(out, tag, value);
    }
}

fn put_count(out: &mut Vec<Field>, tag: u32, count: usize) {
    out.push(Field::new(tag, count.to_string()));
}

macro_rules! flat {
    (
        $(#[$doc:meta])* $name:ident, $kind:literal {
            $($(#[$fdoc:meta])* $mode:ident $field:ident: $ty:ty = $tag:literal,)*
        }
    ) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            $($(#[$fdoc])* pub $field: flat!(@ty $mode $ty),)*
        }

        impl $name {
            /// Type of the message (35).
            pub const MSG_TYPE: &'static str = $kind;

            fn write(&self, out: &mut Vec<Field>) {
                $(flat!(@put $mode out, $tag, self.$field);)*
            }

            fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
                Ok(Self {
                    $($field: flat!(@get $mode reader, $tag),)*
                })
            }
        }
    };
    (@ty req $ty:ty) => { $ty };
    (@ty opt $ty:ty) => { Option<$ty> };
    (@ty lax $ty:ty) => { Option<$ty> };
    (@put req $out:ident, $tag:literal, $value:expr) => { put($out, $tag, &$value) };
    (@put opt $out:ident, $tag:literal, $value:expr) => { put_optional($out, $tag, $value.as_ref()) };
    (@put lax $out:ident, $tag:literal, $value:expr) => { put_optional($out, $tag, $value.as_ref()) };
    (@get req $reader:ident, $tag:literal) => { $reader.required($tag)? };
    (@get opt $reader:ident, $tag:literal) => { $reader.optional($tag)? };
    (@get lax $reader:ident, $tag:literal) => { $reader.lax($tag) };
}

flat! {
    /// Heartbeat (0): sent when nothing else was sent for the heartbeat interval, and in
    /// answer to a test request.
    Heartbeat, "0" {
        /// Identifier of the test request the heartbeat answers (112).
        opt test_req_id: String = 112,
    }
}

flat! {
    /// Test request (1): asks the other side for a heartbeat.
    TestRequest, "1" {
        /// Identifier the answering heartbeat repeats (112).
        req test_req_id: String = 112,
    }
}

flat! {
    /// Resend request (2): asks the other side to send a range of messages again.
    ResendRequest, "2" {
        /// First sequence number of the range (7).
        req begin_seq_no: u64 = 7,
        /// Last sequence number of the range; zero stands for every message after the
        /// first (16).
        req end_seq_no: u64 = 16,
    }
}

flat! {
    /// Reject (3): the other side could not process a message of the session level.
    Reject, "3" {
        /// Sequence number of the rejected message (45).
        req ref_seq_num: u64 = 45,
        /// Explanation (58).
        opt text: String = 58,
        /// Tag the rejection is about (371).
        opt ref_tag_id: u32 = 371,
        /// Type of the rejected message (372).
        opt ref_msg_type: String = 372,
        /// Code of the reason (373).
        opt reason: u32 = 373,
    }
}

flat! {
    /// Sequence reset (4): moves the expected sequence number forward.
    SequenceReset, "4" {
        /// Tells whether the reset fills a gap in answer to a resend request (123).
        opt gap_fill: bool = 123,
        /// Sequence number of the next message (36).
        req new_seq_no: u64 = 36,
    }
}

flat! {
    /// Logout (5): ends the session, or refuses a logon.
    Logout, "5" {
        /// Explanation (58).
        opt text: String = 58,
    }
}

flat! {
    /// Logon (A): opens a session.
    Logon, "A" {
        /// Encryption method; zero, none, is the one supported (98).
        req encrypt_method: u32 = 98,
        /// Heartbeat interval in seconds (108).
        req heart_bt_int: u32 = 108,
        /// Tells whether both sides restart their sequence numbers at one (141).
        opt reset_seq_num: bool = 141,
        /// Login of the account (553).
        opt username: String = 553,
        /// Password of the account (554).
        opt password: FixPassword = 554,
    }
}

flat! {
    /// Business message reject (j): the server refused a message of the application level.
    BusinessReject, "j" {
        /// Sequence number of the refused message (45).
        opt ref_seq_num: u64 = 45,
        /// Type of the refused message (372).
        opt ref_msg_type: String = 372,
        /// Identifier the refused message carried, such as its client order id (379).
        opt ref_id: String = 379,
        /// Code of the reason (380).
        opt reason: u32 = 380,
        /// Explanation (58).
        opt text: String = 58,
    }
}

flat! {
    /// Market data request reject (Y).
    MarketDataReject, "Y" {
        /// Identifier of the refused request (262).
        req md_req_id: String = 262,
        /// Code of the reason; zero stands for an unknown symbol (281).
        opt reason: u32 = 281,
        /// Explanation (58).
        opt text: String = 58,
    }
}

flat! {
    /// Security list request (x).
    SecurityListRequest, "x" {
        /// Identifier of the request (320).
        req security_req_id: String = 320,
        /// Kind of the request; zero asks for the symbols (559).
        req request_type: u32 = 559,
        /// Symbol to restrict the list to (55).
        opt symbol: SymbolId = 55,
    }
}

flat! {
    /// New order single (D).
    NewOrderSingle, "D" {
        /// Identifier the client gives the order (11).
        req cl_ord_id: String = 11,
        /// Symbol (55).
        req symbol: SymbolId = 55,
        /// Side (54).
        req side: Side = 54,
        /// Time the client created the order (60).
        req transact_time: Timestamp = 60,
        /// Quantity (38).
        req order_qty: Volume = 38,
        /// Type (40).
        req ord_type: OrderKind = 40,
        /// Limit price (44).
        opt price: Price = 44,
        /// Stop price (99).
        opt stop_px: Price = 99,
        /// Time in force (59).
        opt time_in_force: TimeInForce = 59,
        /// Expiry time (126).
        opt expire_time: Timestamp = 126,
        /// Position the order changes, on hedged accounts (721).
        opt position_id: PositionId = 721,
        /// Label of the order (494).
        opt designation: String = 494,
    }
}

flat! {
    /// Execution report (8): the state of an order after an event.
    ExecutionReport, "8" {
        /// Identifier the server gave the order (37).
        lax order_id: OrderId = 37,
        /// Identifier the client gave the order (11).
        opt cl_ord_id: String = 11,
        /// Client identifier of the order a cancel or replace request named (41).
        opt orig_cl_ord_id: String = 41,
        /// Number of reports that answer a mass status request (911).
        opt tot_num_reports: u32 = 911,
        /// What the report tells (150).
        req exec_type: ExecType = 150,
        /// State of the order (39).
        req ord_status: OrderStatus = 39,
        /// Symbol (55).
        opt symbol: SymbolId = 55,
        /// Side (54).
        opt side: Side = 54,
        /// Quantity (38).
        opt order_qty: Volume = 38,
        /// Type (40).
        opt ord_type: OrderKind = 40,
        /// Limit price (44).
        opt price: Price = 44,
        /// Stop price (99).
        opt stop_px: Price = 99,
        /// Time in force (59).
        opt time_in_force: TimeInForce = 59,
        /// Expiry time (126).
        opt expire_time: Timestamp = 126,
        /// Quantity of the last fill (32).
        opt last_qty: Volume = 32,
        /// Price of the last fill (31).
        opt last_px: Price = 31,
        /// Quantity still open (151).
        opt leaves_qty: Volume = 151,
        /// Quantity filled so far (14).
        opt cum_qty: Volume = 14,
        /// Average price of the fills (6).
        opt avg_px: Price = 6,
        /// Time of the event (60).
        opt transact_time: Timestamp = 60,
        /// Explanation (58).
        opt text: String = 58,
        /// Code of the reason of a rejection (103).
        opt ord_rej_reason: u32 = 103,
        /// Position the order opened or changed (721).
        opt position_id: PositionId = 721,
        /// Label of the order (494).
        opt designation: String = 494,
        /// Identifier of the mass status request the report answers (584).
        opt mass_status_req_id: String = 584,
        /// Take profit as a price (1000).
        opt absolute_tp: Price = 1000,
        /// Take profit as a distance from the entry price (1001).
        opt relative_tp: Decimal = 1001,
        /// Stop loss as a price (1002).
        opt absolute_sl: Price = 1002,
        /// Stop loss as a distance from the entry price (1003).
        opt relative_sl: Decimal = 1003,
        /// Tells whether the stop loss trails the price (1004).
        opt trailing_sl: bool = 1004,
        /// Code of the trigger method of the stop loss (1005).
        opt trigger_method_sl: u32 = 1005,
        /// Tells whether the stop loss is guaranteed (1006).
        opt guaranteed_sl: bool = 1006,
    }
}

flat! {
    /// Order status request (H).
    OrderStatusRequest, "H" {
        /// Client identifier of the order (11).
        req cl_ord_id: String = 11,
        /// Side of the order (54).
        opt side: Side = 54,
    }
}

flat! {
    /// Order mass status request (AF).
    OrderMassStatusRequest, "AF" {
        /// Identifier of the request (584).
        req mass_status_req_id: String = 584,
        /// Kind of the request; seven asks for every order (585).
        req request_type: u32 = 585,
        /// Earliest issue time of the orders asked for (225).
        opt issue_date: Timestamp = 225,
    }
}

flat! {
    /// Request for positions (AN).
    RequestForPositions, "AN" {
        /// Identifier of the request (710).
        req pos_req_id: String = 710,
        /// Position to restrict the answer to (721).
        opt position_id: PositionId = 721,
    }
}

flat! {
    /// Order cancel request (F).
    OrderCancelRequest, "F" {
        /// Client identifier of the order to cancel (41).
        req orig_cl_ord_id: String = 41,
        /// Server identifier of the order to cancel (37).
        opt order_id: OrderId = 37,
        /// Client identifier of the request (11).
        req cl_ord_id: String = 11,
    }
}

flat! {
    /// Order cancel reject (9).
    OrderCancelReject, "9" {
        /// Server identifier of the order (37).
        lax order_id: OrderId = 37,
        /// Client identifier of the refused request (11).
        req cl_ord_id: String = 11,
        /// Client identifier of the order the request named (41).
        opt orig_cl_ord_id: String = 41,
        /// State of the order (39).
        req ord_status: OrderStatus = 39,
        /// Request the reject answers (434).
        req response_to: CancelRejectResponse = 434,
        /// Explanation (58).
        opt text: String = 58,
    }
}

flat! {
    /// Order cancel/replace request (G).
    OrderCancelReplaceRequest, "G" {
        /// Client identifier of the order to change (41).
        req orig_cl_ord_id: String = 41,
        /// Server identifier of the order to change (37).
        opt order_id: OrderId = 37,
        /// Client identifier of the request (11).
        req cl_ord_id: String = 11,
        /// New quantity (38).
        req order_qty: Volume = 38,
        /// New limit price (44).
        opt price: Price = 44,
        /// New stop price (99).
        opt stop_px: Price = 99,
        /// New expiry time (126).
        opt expire_time: Timestamp = 126,
    }
}

const TAG_SYMBOL: u32 = 55;
const TAG_LONG_QTY: u32 = 704;
const TAG_SHORT_QTY: u32 = 705;
const TAG_NO_RELATED_SYM: u32 = 146;
const TAG_MD_REQ_ID: u32 = 262;
const TAG_SUBSCRIPTION: u32 = 263;
const TAG_MARKET_DEPTH: u32 = 264;
const TAG_UPDATE_TYPE: u32 = 265;
const TAG_NO_ENTRY_TYPES: u32 = 267;
const TAG_NO_ENTRIES: u32 = 268;
const TAG_ENTRY_TYPE: u32 = 269;
const TAG_ENTRY_PX: u32 = 270;
const TAG_ENTRY_SIZE: u32 = 271;
const TAG_ENTRY_ID: u32 = 278;
const TAG_UPDATE_ACTION: u32 = 279;
const TAG_QUOTE_ENTRY_ID: u32 = 299;
const TAG_SECURITY_REQ_ID: u32 = 320;
const TAG_SECURITY_RESPONSE_ID: u32 = 322;
const TAG_SECURITY_RESULT: u32 = 560;
const TAG_NO_POSITIONS: u32 = 702;
const TAG_POS_REQ_ID: u32 = 710;
const TAG_POSITION_ID: u32 = 721;
const TAG_TOTAL_REPORTS: u32 = 727;
const TAG_POS_REQ_RESULT: u32 = 728;
const TAG_SETTL_PRICE: u32 = 730;
const TAG_SYMBOL_NAME: u32 = 1007;
const TAG_SYMBOL_DIGITS: u32 = 1008;
const TAG_ABSOLUTE_TP: u32 = 1000;
const TAG_ABSOLUTE_SL: u32 = 1002;
const TAG_TRAILING_SL: u32 = 1004;
const TAG_TRIGGER_METHOD_SL: u32 = 1005;
const TAG_GUARANTEED_SL: u32 = 1006;

const SUBSCRIBE: &str = "1";
const UNSUBSCRIBE: &str = "2";
const INCREMENTAL_REFRESH: &str = "1";
const ACTION_NEW: &str = "0";
const ACTION_DELETE: &str = "2";

/// Market data request (V): subscribes to or unsubscribes from prices of symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataRequest {
    /// Identifier of the request; an unsubscription repeats the one of the subscription (262).
    pub md_req_id: String,
    /// Tells whether the request subscribes; otherwise it unsubscribes (263).
    pub subscribe: bool,
    /// Depth asked for (264).
    pub depth: MarketDepth,
    /// Symbols (146, 55).
    pub symbols: Vec<SymbolId>,
    /// Sides asked for (267, 269).
    pub sides: Vec<BookSide>,
}

impl MarketDataRequest {
    /// Type of the message (35).
    pub const MSG_TYPE: &'static str = "V";

    fn write(&self, out: &mut Vec<Field>) {
        put(out, TAG_MD_REQ_ID, &self.md_req_id);
        let subscription = if self.subscribe {
            SUBSCRIBE
        } else {
            UNSUBSCRIBE
        };
        out.push(Field::new(TAG_SUBSCRIPTION, subscription));
        put(out, TAG_MARKET_DEPTH, &self.depth);
        out.push(Field::new(TAG_UPDATE_TYPE, INCREMENTAL_REFRESH));
        put_count(out, TAG_NO_RELATED_SYM, self.symbols.len());
        for symbol in &self.symbols {
            put(out, TAG_SYMBOL, symbol);
        }
        put_count(out, TAG_NO_ENTRY_TYPES, self.sides.len());
        for side in &self.sides {
            put(out, TAG_ENTRY_TYPE, side);
        }
    }

    fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
        let subscribe = match reader.required::<String>(TAG_SUBSCRIPTION)?.as_str() {
            SUBSCRIBE => true,
            UNSUBSCRIBE => false,
            _ => return Err(reader.invalid(TAG_SUBSCRIPTION)),
        };
        Ok(Self {
            md_req_id: reader.required(TAG_MD_REQ_ID)?,
            subscribe,
            depth: reader.required(TAG_MARKET_DEPTH)?,
            symbols: reader
                .group(TAG_NO_RELATED_SYM, &[TAG_SYMBOL])?
                .iter()
                .map(|entry| entry.required(TAG_SYMBOL))
                .collect::<Result<_, _>>()?,
            sides: reader
                .group(TAG_NO_ENTRY_TYPES, &[TAG_ENTRY_TYPE])?
                .iter()
                .map(|entry| entry.required(TAG_ENTRY_TYPE))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// One price of a market data snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// Side (269).
    pub side: BookSide,
    /// Price (270).
    pub price: Price,
    /// Quantity offered at the price; absent for spot prices (271).
    pub size: Option<Volume>,
    /// Identifier of the entry in the book; absent for spot prices (278).
    pub id: Option<String>,
}

/// Market data snapshot (W): the prices of one symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataSnapshot {
    /// Identifier of the request the snapshot belongs to (262).
    pub md_req_id: Option<String>,
    /// Symbol (55).
    pub symbol: SymbolId,
    /// Prices (268).
    pub entries: Vec<SnapshotEntry>,
}

impl MarketDataSnapshot {
    /// Type of the message (35).
    pub const MSG_TYPE: &'static str = "W";

    fn write(&self, out: &mut Vec<Field>) {
        put_optional(out, TAG_MD_REQ_ID, self.md_req_id.as_ref());
        put(out, TAG_SYMBOL, &self.symbol);
        put_count(out, TAG_NO_ENTRIES, self.entries.len());
        for entry in &self.entries {
            put(out, TAG_ENTRY_TYPE, &entry.side);
            put(out, TAG_ENTRY_PX, &entry.price);
            put_optional(out, TAG_ENTRY_SIZE, entry.size.as_ref());
            put_optional(out, TAG_ENTRY_ID, entry.id.as_ref());
        }
    }

    fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
        let members = [
            TAG_ENTRY_TYPE,
            TAG_QUOTE_ENTRY_ID,
            TAG_ENTRY_PX,
            TAG_ENTRY_SIZE,
            TAG_ENTRY_ID,
        ];
        Ok(Self {
            md_req_id: reader.optional(TAG_MD_REQ_ID)?,
            symbol: reader.required(TAG_SYMBOL)?,
            entries: reader
                .group(TAG_NO_ENTRIES, &members)?
                .iter()
                .map(|entry| {
                    Ok(SnapshotEntry {
                        side: entry.required(TAG_ENTRY_TYPE)?,
                        price: entry.required(TAG_ENTRY_PX)?,
                        size: entry.optional(TAG_ENTRY_SIZE)?,
                        id: entry.optional(TAG_ENTRY_ID)?,
                    })
                })
                .collect::<Result<_, FixError>>()?,
        })
    }
}

/// One change of a book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookUpdate {
    /// A new entry.
    New {
        /// Side (269).
        side: BookSide,
        /// Identifier of the entry in the book (278).
        id: String,
        /// Symbol (55).
        symbol: SymbolId,
        /// Price (270).
        price: Price,
        /// Quantity offered at the price (271).
        size: Option<Volume>,
    },
    /// The removal of an entry.
    Delete {
        /// Identifier of the entry in the book (278).
        id: String,
        /// Symbol (55).
        symbol: SymbolId,
    },
}

/// Market data incremental refresh (X): changes of books.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataIncremental {
    /// Identifier of the request the changes belong to (262).
    pub md_req_id: Option<String>,
    /// Changes in the order they apply (268).
    pub entries: Vec<BookUpdate>,
}

impl MarketDataIncremental {
    /// Type of the message (35).
    pub const MSG_TYPE: &'static str = "X";

    fn write(&self, out: &mut Vec<Field>) {
        put_optional(out, TAG_MD_REQ_ID, self.md_req_id.as_ref());
        put_count(out, TAG_NO_ENTRIES, self.entries.len());
        for entry in &self.entries {
            match entry {
                BookUpdate::New {
                    side,
                    id,
                    symbol,
                    price,
                    size,
                } => {
                    out.push(Field::new(TAG_UPDATE_ACTION, ACTION_NEW));
                    put(out, TAG_ENTRY_TYPE, side);
                    put(out, TAG_ENTRY_ID, id);
                    put(out, TAG_SYMBOL, symbol);
                    put(out, TAG_ENTRY_PX, price);
                    put_optional(out, TAG_ENTRY_SIZE, size.as_ref());
                }
                BookUpdate::Delete { id, symbol } => {
                    out.push(Field::new(TAG_UPDATE_ACTION, ACTION_DELETE));
                    put(out, TAG_ENTRY_ID, id);
                    put(out, TAG_SYMBOL, symbol);
                }
            }
        }
    }

    fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
        let members = [
            TAG_UPDATE_ACTION,
            TAG_ENTRY_TYPE,
            TAG_ENTRY_ID,
            TAG_SYMBOL,
            TAG_ENTRY_PX,
            TAG_ENTRY_SIZE,
        ];
        Ok(Self {
            md_req_id: reader.optional(TAG_MD_REQ_ID)?,
            entries: reader
                .group(TAG_NO_ENTRIES, &members)?
                .iter()
                .map(|entry| {
                    let id = entry.required(TAG_ENTRY_ID)?;
                    let symbol = entry.required(TAG_SYMBOL)?;
                    match entry.required::<String>(TAG_UPDATE_ACTION)?.as_str() {
                        ACTION_NEW => Ok(BookUpdate::New {
                            side: entry.required(TAG_ENTRY_TYPE)?,
                            id,
                            symbol,
                            price: entry.required(TAG_ENTRY_PX)?,
                            size: entry.optional(TAG_ENTRY_SIZE)?,
                        }),
                        ACTION_DELETE => Ok(BookUpdate::Delete { id, symbol }),
                        _ => Err(entry.invalid(TAG_UPDATE_ACTION)),
                    }
                })
                .collect::<Result<_, FixError>>()?,
        })
    }
}

/// One symbol of a security list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityEntry {
    /// Identifier of the symbol (55).
    pub symbol: SymbolId,
    /// Name of the symbol (1007).
    pub name: Option<String>,
    /// Number of decimal places of its prices (1008).
    pub digits: Option<u32>,
}

/// Security list (y): the symbols of the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityList {
    /// Identifier of the request the list answers (320).
    pub security_req_id: String,
    /// Identifier the server gave the answer (322).
    pub security_response_id: String,
    /// Code of the result; zero stands for a valid request (560).
    pub result: u32,
    /// Symbols (146).
    pub symbols: Vec<SecurityEntry>,
}

impl SecurityList {
    /// Type of the message (35).
    pub const MSG_TYPE: &'static str = "y";

    fn write(&self, out: &mut Vec<Field>) {
        put(out, TAG_SECURITY_REQ_ID, &self.security_req_id);
        put(out, TAG_SECURITY_RESPONSE_ID, &self.security_response_id);
        put(out, TAG_SECURITY_RESULT, &self.result);
        if !self.symbols.is_empty() {
            put_count(out, TAG_NO_RELATED_SYM, self.symbols.len());
        }
        for entry in &self.symbols {
            put(out, TAG_SYMBOL, &entry.symbol);
            put_optional(out, TAG_SYMBOL_NAME, entry.name.as_ref());
            put_optional(out, TAG_SYMBOL_DIGITS, entry.digits.as_ref());
        }
    }

    fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
        let members = [TAG_SYMBOL, TAG_SYMBOL_NAME, TAG_SYMBOL_DIGITS];
        Ok(Self {
            security_req_id: reader.required(TAG_SECURITY_REQ_ID)?,
            security_response_id: reader.required(TAG_SECURITY_RESPONSE_ID)?,
            result: reader.required(TAG_SECURITY_RESULT)?,
            symbols: reader
                .group(TAG_NO_RELATED_SYM, &members)?
                .iter()
                .map(|entry| {
                    Ok(SecurityEntry {
                        symbol: entry.required(TAG_SYMBOL)?,
                        name: entry.optional(TAG_SYMBOL_NAME)?,
                        digits: entry.optional(TAG_SYMBOL_DIGITS)?,
                    })
                })
                .collect::<Result<_, FixError>>()?,
        })
    }
}

/// Position report (AP): one open position, or the news that there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionReport {
    /// Identifier of the request the report answers (710).
    pub pos_req_id: String,
    /// Identifier of the position (721).
    pub position_id: Option<PositionId>,
    /// Number of reports that answer the request (727).
    pub total_reports: u32,
    /// Code of the result; zero stands for a valid request, two for no position (728).
    pub result: u32,
    /// Symbol (55).
    pub symbol: Option<SymbolId>,
    /// Entry price of the position (730).
    pub settl_price: Option<Price>,
    /// Quantity bought (704).
    pub long_qty: Option<Volume>,
    /// Quantity sold (705).
    pub short_qty: Option<Volume>,
    /// Take profit as a price (1000).
    pub absolute_tp: Option<Price>,
    /// Stop loss as a price (1002).
    pub absolute_sl: Option<Price>,
    /// Tells whether the stop loss trails the price (1004).
    pub trailing_sl: Option<bool>,
    /// Code of the trigger method of the stop loss (1005).
    pub trigger_method_sl: Option<u32>,
    /// Tells whether the stop loss is guaranteed (1006).
    pub guaranteed_sl: Option<bool>,
}

impl PositionReport {
    /// Type of the message (35).
    pub const MSG_TYPE: &'static str = "AP";

    fn write(&self, out: &mut Vec<Field>) {
        put(out, TAG_POS_REQ_ID, &self.pos_req_id);
        put_optional(out, TAG_POSITION_ID, self.position_id.as_ref());
        put(out, TAG_TOTAL_REPORTS, &self.total_reports);
        put(out, TAG_POS_REQ_RESULT, &self.result);
        put_optional(out, TAG_SYMBOL, self.symbol.as_ref());
        put_optional(out, TAG_SETTL_PRICE, self.settl_price.as_ref());
        if self.long_qty.is_some() || self.short_qty.is_some() {
            put_count(out, TAG_NO_POSITIONS, 1);
        }
        put_optional(out, TAG_LONG_QTY, self.long_qty.as_ref());
        put_optional(out, TAG_SHORT_QTY, self.short_qty.as_ref());
        put_optional(out, TAG_ABSOLUTE_TP, self.absolute_tp.as_ref());
        put_optional(out, TAG_ABSOLUTE_SL, self.absolute_sl.as_ref());
        put_optional(out, TAG_TRAILING_SL, self.trailing_sl.as_ref());
        put_optional(out, TAG_TRIGGER_METHOD_SL, self.trigger_method_sl.as_ref());
        put_optional(out, TAG_GUARANTEED_SL, self.guaranteed_sl.as_ref());
    }

    fn read(reader: &Reader<'_>) -> Result<Self, FixError> {
        Ok(Self {
            pos_req_id: reader.required(TAG_POS_REQ_ID)?,
            position_id: reader.optional(TAG_POSITION_ID)?,
            total_reports: reader.required(TAG_TOTAL_REPORTS)?,
            result: reader.required(TAG_POS_REQ_RESULT)?,
            symbol: reader.optional(TAG_SYMBOL)?,
            settl_price: reader.optional(TAG_SETTL_PRICE)?,
            long_qty: reader.optional(TAG_LONG_QTY)?,
            short_qty: reader.optional(TAG_SHORT_QTY)?,
            absolute_tp: reader.optional(TAG_ABSOLUTE_TP)?,
            absolute_sl: reader.optional(TAG_ABSOLUTE_SL)?,
            trailing_sl: reader.optional(TAG_TRAILING_SL)?,
            trigger_method_sl: reader.optional(TAG_TRIGGER_METHOD_SL)?,
            guaranteed_sl: reader.optional(TAG_GUARANTEED_SL)?,
        })
    }
}

macro_rules! message_set {
    ($($name:ident: $doc:literal,)*) => {
        /// Any message the library sends or understands.
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum Message {
            $(#[doc = $doc] $name($name),)*
            /// Execution report (8).
            ExecutionReport(Box<ExecutionReport>),
        }

        impl Message {
            /// Returns the type of the message (35).
            #[must_use]
            pub const fn msg_type(&self) -> &'static str {
                match self {
                    $(Self::$name(_) => $name::MSG_TYPE,)*
                    Self::ExecutionReport(_) => ExecutionReport::MSG_TYPE,
                }
            }

            /// Returns the fields of the body in the order of the wire.
            #[must_use]
            pub fn body(&self) -> Vec<Field> {
                let mut out = Vec::new();
                match self {
                    $(Self::$name(message) => message.write(&mut out),)*
                    Self::ExecutionReport(message) => message.write(&mut out),
                }
                out
            }

            /// Parses the body of a message of the given type.
            ///
            /// # Errors
            ///
            /// Returns [`FixError::UnsupportedMessage`] for a type the library does not know,
            /// [`FixError::MissingField`] when a required field is absent and
            /// [`FixError::InvalidField`] when a value has the wrong form.
            pub fn parse(msg_type: &str, body: &[Field]) -> Result<Self, FixError> {
                $(if msg_type == $name::MSG_TYPE {
                    return $name::read(&Reader::new($name::MSG_TYPE, body)).map(Self::$name);
                })*
                if msg_type == ExecutionReport::MSG_TYPE {
                    return ExecutionReport::read(&Reader::new(ExecutionReport::MSG_TYPE, body))
                        .map(|report| Self::ExecutionReport(Box::new(report)));
                }
                Err(FixError::UnsupportedMessage(msg_type.to_owned()))
            }
        }

        $(impl From<$name> for Message {
            fn from(message: $name) -> Self {
                Self::$name(message)
            }
        })*
    };
}

message_set! {
    Heartbeat: "Heartbeat (0).",
    TestRequest: "Test request (1).",
    ResendRequest: "Resend request (2).",
    Reject: "Reject (3).",
    SequenceReset: "Sequence reset (4).",
    Logout: "Logout (5).",
    Logon: "Logon (A).",
    BusinessReject: "Business message reject (j).",
    MarketDataRequest: "Market data request (V).",
    MarketDataSnapshot: "Market data snapshot (W).",
    MarketDataIncremental: "Market data incremental refresh (X).",
    MarketDataReject: "Market data request reject (Y).",
    SecurityListRequest: "Security list request (x).",
    SecurityList: "Security list (y).",
    NewOrderSingle: "New order single (D).",
    OrderStatusRequest: "Order status request (H).",
    OrderMassStatusRequest: "Order mass status request (AF).",
    RequestForPositions: "Request for positions (AN).",
    PositionReport: "Position report (AP).",
    OrderCancelRequest: "Order cancel request (F).",
    OrderCancelReject: "Order cancel reject (9).",
    OrderCancelReplaceRequest: "Order cancel/replace request (G).",
}

impl From<ExecutionReport> for Message {
    fn from(message: ExecutionReport) -> Self {
        Self::ExecutionReport(Box::new(message))
    }
}

impl Message {
    /// Builds the frame of the message under the given header, whose message type is
    /// replaced by the one of the message.
    #[must_use]
    pub fn to_frame(&self, mut header: Header) -> Frame {
        self.msg_type().clone_into(&mut header.msg_type);
        Frame::new(header, self.body())
    }

    /// Parses the message a frame carries.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Message::parse`].
    pub fn from_frame(frame: &Frame) -> Result<Self, FixError> {
        Self::parse(&frame.header.msg_type, &frame.body)
    }

    /// Tells whether the message belongs to the session level: it is never sent again as
    /// it was, a sequence reset stands in for it.
    #[must_use]
    pub const fn is_administrative(&self) -> bool {
        matches!(
            self,
            Self::Heartbeat(_)
                | Self::TestRequest(_)
                | Self::ResendRequest(_)
                | Self::Reject(_)
                | Self::SequenceReset(_)
                | Self::Logout(_)
                | Self::Logon(_)
        )
    }
}

#[cfg(test)]
#[path = "messages_tests.rs"]
mod tests;

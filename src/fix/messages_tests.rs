use std::str::FromStr;

use rust_decimal::Decimal;

use super::{
    BookSide, BookUpdate, BusinessReject, CancelRejectResponse, ExecType, ExecutionReport,
    Heartbeat, Logon, Logout, MarketDataIncremental, MarketDataReject, MarketDataRequest,
    MarketDataSnapshot, MarketDepth, Message, NewOrderSingle, OrderCancelReject,
    OrderCancelReplaceRequest, OrderCancelRequest, OrderKind, OrderMassStatusRequest, OrderStatus,
    OrderStatusRequest, PositionReport, Reject, RequestForPositions, ResendRequest, SecurityEntry,
    SecurityList, SecurityListRequest, SequenceReset, Side, SnapshotEntry, TestRequest,
    TimeInForce,
};
use crate::common::config::FixPassword;
use crate::common::decimals::{Price, Volume};
use crate::common::errors::FixError;
use crate::common::ids::{OrderId, PositionId, SymbolId};
use crate::common::time::Timestamp;
use crate::fix::codec::{BEGIN_STRING, Field, Header, checksum, decode, encode};

fn text(value: &str) -> String {
    value.to_owned()
}

fn price(value: &str) -> Price {
    Price::from_str(value).expect("price")
}

fn volume(value: &str) -> Volume {
    Volume::from_str(value).expect("volume")
}

fn symbol(value: i64) -> SymbolId {
    SymbolId::new(value).expect("symbol id")
}

fn order(value: i64) -> OrderId {
    OrderId::new(value).expect("order id")
}

fn position(value: i64) -> PositionId {
    PositionId::new(value).expect("position id")
}

fn moment(millis: i64) -> Timestamp {
    Timestamp::from_millis(millis).expect("moment")
}

fn execution_report() -> ExecutionReport {
    ExecutionReport {
        order_id: Some(order(101)),
        cl_ord_id: Some(text("order-1")),
        orig_cl_ord_id: Some(text("order-0")),
        tot_num_reports: Some(2),
        exec_type: ExecType::Trade,
        ord_status: OrderStatus::PartiallyFilled,
        symbol: Some(symbol(1)),
        side: Some(Side::Buy),
        order_qty: Some(volume("10000")),
        ord_type: Some(OrderKind::StopLimit),
        price: Some(price("1.23456")),
        stop_px: Some(price("1.2")),
        time_in_force: Some(TimeInForce::GoodTillDate),
        expire_time: Some(moment(1_484_640_184_000)),
        last_qty: Some(volume("0.01")),
        last_px: Some(price("1.23")),
        leaves_qty: Some(volume("9999.99")),
        cum_qty: Some(volume("0.01")),
        avg_px: Some(price("1.23")),
        transact_time: Some(moment(1_484_640_184_155)),
        text: Some(text("filled in part")),
        ord_rej_reason: Some(0),
        position_id: Some(position(77)),
        designation: Some(text("label")),
        mass_status_req_id: Some(text("mass-1")),
        absolute_tp: Some(price("1.3")),
        relative_tp: Some(Decimal::new(5, 2)),
        absolute_sl: Some(price("1.1")),
        relative_sl: Some(Decimal::new(-5, 2)),
        trailing_sl: Some(true),
        trigger_method_sl: Some(1),
        guaranteed_sl: Some(false),
    }
}

fn bare_execution_report() -> ExecutionReport {
    ExecutionReport {
        order_id: None,
        cl_ord_id: None,
        orig_cl_ord_id: None,
        tot_num_reports: None,
        exec_type: ExecType::Rejected,
        ord_status: OrderStatus::Rejected,
        symbol: None,
        side: None,
        order_qty: None,
        ord_type: None,
        price: None,
        stop_px: None,
        time_in_force: None,
        expire_time: None,
        last_qty: None,
        last_px: None,
        leaves_qty: None,
        cum_qty: None,
        avg_px: None,
        transact_time: None,
        text: None,
        ord_rej_reason: None,
        position_id: None,
        designation: None,
        mass_status_req_id: None,
        absolute_tp: None,
        relative_tp: None,
        absolute_sl: None,
        relative_sl: None,
        trailing_sl: None,
        trigger_method_sl: None,
        guaranteed_sl: None,
    }
}

fn position_report() -> PositionReport {
    PositionReport {
        pos_req_id: text("positions-1"),
        position_id: Some(position(77)),
        total_reports: 1,
        result: 0,
        symbol: Some(symbol(1)),
        settl_price: Some(price("1.23")),
        long_qty: Some(volume("1000")),
        short_qty: Some(volume("0")),
        absolute_tp: Some(price("1.3")),
        absolute_sl: Some(price("1.1")),
        trailing_sl: Some(false),
        trigger_method_sl: Some(2),
        guaranteed_sl: Some(true),
    }
}

fn session_samples() -> Vec<Message> {
    vec![
        Heartbeat { test_req_id: None }.into(),
        Heartbeat {
            test_req_id: Some(text("ping")),
        }
        .into(),
        TestRequest {
            test_req_id: text("ping"),
        }
        .into(),
        ResendRequest {
            begin_seq_no: 3,
            end_seq_no: 0,
        }
        .into(),
        Reject {
            ref_seq_num: 4,
            text: Some(text("bad tag")),
            ref_tag_id: Some(55),
            ref_msg_type: Some(text("D")),
            reason: Some(5),
        }
        .into(),
        SequenceReset {
            gap_fill: Some(true),
            new_seq_no: 9,
        }
        .into(),
        SequenceReset {
            gap_fill: None,
            new_seq_no: 9,
        }
        .into(),
        Logout {
            text: Some(text("bye")),
        }
        .into(),
        Logout { text: None }.into(),
        Logon {
            encrypt_method: 0,
            heart_bt_int: 30,
            reset_seq_num: Some(true),
            username: Some(text("12345")),
            password: Some(FixPassword::new("passw0rd!").expect("password")),
        }
        .into(),
        BusinessReject {
            ref_seq_num: Some(5),
            ref_msg_type: Some(text("AF")),
            ref_id: Some(text("mass-1")),
            reason: Some(0),
            text: Some(text("ORDER_NOT_FOUND:no orders")),
        }
        .into(),
    ]
}

fn market_samples() -> Vec<Message> {
    vec![
        MarketDataRequest {
            md_req_id: text("md-1"),
            subscribe: true,
            depth: MarketDepth::Spot,
            symbols: vec![symbol(1), symbol(2)],
            sides: vec![BookSide::Bid, BookSide::Offer],
        }
        .into(),
        MarketDataRequest {
            md_req_id: text("md-1"),
            subscribe: false,
            depth: MarketDepth::Full,
            symbols: vec![symbol(1)],
            sides: vec![BookSide::Bid, BookSide::Offer],
        }
        .into(),
        MarketDataSnapshot {
            md_req_id: Some(text("md-1")),
            symbol: symbol(1),
            entries: vec![
                SnapshotEntry {
                    side: BookSide::Bid,
                    price: price("1.06625"),
                    size: None,
                    id: None,
                },
                SnapshotEntry {
                    side: BookSide::Offer,
                    price: price("1.0663"),
                    size: Some(volume("1000000")),
                    id: Some(text("7491")),
                },
            ],
        }
        .into(),
        MarketDataSnapshot {
            md_req_id: None,
            symbol: symbol(1),
            entries: Vec::new(),
        }
        .into(),
        MarketDataIncremental {
            md_req_id: None,
            entries: vec![
                BookUpdate::New {
                    side: BookSide::Bid,
                    id: text("7491"),
                    symbol: symbol(1),
                    price: price("1.06625"),
                    size: Some(volume("2000000")),
                },
                BookUpdate::Delete {
                    id: text("7490"),
                    symbol: symbol(1),
                },
                BookUpdate::New {
                    side: BookSide::Offer,
                    id: text("7492"),
                    symbol: symbol(1),
                    price: price("1.0663"),
                    size: None,
                },
            ],
        }
        .into(),
        MarketDataReject {
            md_req_id: text("md-1"),
            reason: Some(0),
            text: Some(text("INVALID_REQUEST: no such symbol")),
        }
        .into(),
    ]
}

fn security_samples() -> Vec<Message> {
    vec![
        SecurityListRequest {
            security_req_id: text("list-1"),
            request_type: 0,
            symbol: Some(symbol(1)),
        }
        .into(),
        SecurityList {
            security_req_id: text("list-1"),
            security_response_id: text("response-1"),
            result: 0,
            symbols: vec![
                SecurityEntry {
                    symbol: symbol(1),
                    name: Some(text("EURUSD")),
                    digits: Some(5),
                },
                SecurityEntry {
                    symbol: symbol(2),
                    name: None,
                    digits: None,
                },
            ],
        }
        .into(),
        SecurityList {
            security_req_id: text("list-1"),
            security_response_id: text("response-1"),
            result: 2,
            symbols: Vec::new(),
        }
        .into(),
    ]
}

fn trade_samples() -> Vec<Message> {
    vec![
        NewOrderSingle {
            cl_ord_id: text("order-1"),
            symbol: symbol(1),
            side: Side::Sell,
            transact_time: moment(1_484_640_184_000),
            order_qty: volume("0.01"),
            ord_type: OrderKind::Limit,
            price: Some(price("1.5")),
            stop_px: Some(price("1.4")),
            time_in_force: Some(TimeInForce::GoodTillCancel),
            expire_time: Some(moment(1_484_640_184_000)),
            position_id: Some(position(77)),
            designation: Some(text("label")),
        }
        .into(),
        execution_report().into(),
        bare_execution_report().into(),
        OrderStatusRequest {
            cl_ord_id: text("order-1"),
            side: Some(Side::Buy),
        }
        .into(),
        OrderMassStatusRequest {
            mass_status_req_id: text("mass-1"),
            request_type: 7,
            issue_date: Some(moment(1_484_640_184_000)),
        }
        .into(),
        RequestForPositions {
            pos_req_id: text("positions-1"),
            position_id: Some(position(77)),
        }
        .into(),
        position_report().into(),
        PositionReport {
            pos_req_id: text("positions-1"),
            position_id: None,
            total_reports: 0,
            result: 2,
            symbol: None,
            settl_price: None,
            long_qty: None,
            short_qty: None,
            absolute_tp: None,
            absolute_sl: None,
            trailing_sl: None,
            trigger_method_sl: None,
            guaranteed_sl: None,
        }
        .into(),
        OrderCancelRequest {
            orig_cl_ord_id: text("order-1"),
            order_id: Some(order(101)),
            cl_ord_id: text("cancel-1"),
        }
        .into(),
        OrderCancelReject {
            order_id: Some(order(101)),
            cl_ord_id: text("cancel-1"),
            orig_cl_ord_id: Some(text("order-1")),
            ord_status: OrderStatus::Filled,
            response_to: CancelRejectResponse::Cancel,
            text: Some(text("too late")),
        }
        .into(),
        OrderCancelReplaceRequest {
            orig_cl_ord_id: text("order-1"),
            order_id: Some(order(101)),
            cl_ord_id: text("replace-1"),
            order_qty: volume("0.02"),
            price: Some(price("1.6")),
            stop_px: Some(price("1.5")),
            expire_time: Some(moment(1_484_640_184_000)),
        }
        .into(),
    ]
}

fn samples() -> Vec<Message> {
    let mut all = session_samples();
    all.extend(market_samples());
    all.extend(security_samples());
    all.extend(trade_samples());
    all
}

fn header(seq: u64) -> Header {
    Header::new(
        "",
        "demo.broker.1",
        "CSERVER",
        seq,
        moment(1_484_640_184_155),
    )
    .qualified("TRADE", "TRADE")
}

fn sealed(body: &str) -> Vec<u8> {
    let body = body.replace('|', "\u{1}");
    let mut out = format!("8={BEGIN_STRING}\u{1}9={}\u{1}{body}", body.len()).into_bytes();
    let sum = checksum(&out);
    out.extend_from_slice(format!("10={sum:03}\u{1}").as_bytes());
    out
}

fn received(body: &str) -> Result<Message, FixError> {
    let mut bytes = sealed(body);
    let frame = decode(&mut bytes)?.expect("whole message");
    Message::from_frame(&frame)
}

#[test]
fn every_supported_message_round_trips_through_the_wire() {
    let samples = samples();
    let mut kinds: Vec<&str> = samples.iter().map(Message::msg_type).collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(
        kinds,
        vec![
            "0", "1", "2", "3", "4", "5", "8", "9", "A", "AF", "AN", "AP", "D", "F", "G", "H", "V",
            "W", "X", "Y", "j", "x", "y"
        ]
    );
    let mut stream = Vec::new();
    for (index, message) in samples.iter().enumerate() {
        let seq = u64::try_from(index).expect("index") + 1;
        stream.extend_from_slice(&encode(&message.to_frame(header(seq))).expect("encode"));
    }
    for (index, message) in samples.iter().enumerate() {
        let frame = decode(&mut stream).expect("decode").expect("whole message");
        assert_eq!(frame.header.msg_type, message.msg_type());
        assert_eq!(frame.header.seq, u64::try_from(index).expect("index") + 1);
        assert_eq!(&Message::from_frame(&frame).expect("parse"), message);
    }
    assert!(stream.is_empty());
}

#[test]
fn documented_server_messages_are_parsed() {
    let logon = received(
        "35=A|34=1|49=CSERVER|50=TRADE|52=20170117-08:03:04.509|56=live.theBroker.12345|57=any_string|98=0|108=30|141=Y|",
    );
    assert_eq!(
        logon,
        Ok(Logon {
            encrypt_method: 0,
            heart_bt_int: 30,
            reset_seq_num: Some(true),
            username: None,
            password: None,
        }
        .into())
    );
    let spot = received(
        "35=W|34=2|49=CSERVER|50=QUOTE|52=20170117-10:26:54.630|56=live.theBroker.12345|57=any_string|55=1|268=2|269=0|270=1.06625|269=1|270=1.0663|",
    )
    .expect("spot snapshot");
    let Message::MarketDataSnapshot(spot) = spot else {
        panic!("not a snapshot");
    };
    assert_eq!(spot.symbol, symbol(1));
    assert_eq!(spot.entries.len(), 2);
    assert_eq!(
        spot.entries.first().map(|e| e.price),
        Some(price("1.06625"))
    );
    let refresh = received(
        "35=X|34=3|49=CSERVER|50=QUOTE|52=20170117-10:26:54.630|56=live.theBroker.12345|57=any_string|268=2|279=0|269=0|278=7491|55=1|270=1.06625|271=2000000|279=2|278=7490|55=1|",
    )
    .expect("incremental refresh");
    let Message::MarketDataIncremental(refresh) = refresh else {
        panic!("not a refresh");
    };
    assert_eq!(
        refresh.entries.last(),
        Some(&BookUpdate::Delete {
            id: text("7490"),
            symbol: symbol(1)
        })
    );
    let positions = received(
        "35=AP|34=4|49=CSERVER|50=TRADE|52=20170117-10:26:54.630|56=live.theBroker.12345|57=any_string|55=1|710=876316403|721=101|727=1|728=0|730=1.06671|702=1|704=0|705=30000|",
    )
    .expect("position report");
    let Message::PositionReport(positions) = positions else {
        panic!("not a position report");
    };
    assert_eq!(positions.position_id, Some(position(101)));
    assert_eq!(positions.short_qty, Some(volume("30000")));
    assert_eq!(positions.long_qty, Some(volume("0")));
    let filled = received(
        "35=8|34=5|49=CSERVER|50=TRADE|52=20170117-10:02:14.720|56=live.theBroker.12345|57=any_string|6=1.0674|11=876316397|14=10000|32=10000|37=101|38=10000|39=2|40=1|54=1|55=1|59=3|60=20170117-10:02:14.619|150=F|151=0|721=101|",
    )
    .expect("execution report");
    let Message::ExecutionReport(filled) = filled else {
        panic!("not an execution report");
    };
    assert_eq!(filled.exec_type, ExecType::Trade);
    assert_eq!(filled.ord_status, OrderStatus::Filled);
    assert_eq!(filled.order_id, Some(order(101)));
    assert_eq!(filled.avg_px, Some(price("1.0674")));
    assert_eq!(filled.time_in_force, Some(TimeInForce::ImmediateOrCancel));
    assert_eq!(filled.transact_time, Some(moment(1_484_647_334_619)));
}

#[test]
fn missing_and_invalid_fields_reject_the_whole_message() {
    let head = "34=1|49=CSERVER|52=20170117-08:03:04|56=me|";
    let cases = [
        ("1", "", true, 112),
        ("A", "98=0|", true, 108),
        ("A", "98=x|108=30|", false, 98),
        ("A", "98=0|108=30|141=1|", false, 141),
        ("2", "7=1|16=-1|", false, 16),
        ("8", "150=F|", true, 39),
        ("8", "150=Z|39=0|", false, 150),
        ("8", "150=0|39=0|44=-1|", false, 44),
        ("8", "150=0|39=0|60=yesterday|", false, 60),
        ("8", "150=0|39=0|55=EURUSD|", false, 55),
        ("W", "268=0|", true, 55),
        ("W", "55=1|268=2|269=0|270=1|", false, 268),
        ("W", "55=1|268=x|", false, 268),
        ("W", "55=1|268=1|270=1|269=0|", false, 268),
        ("W", "55=1|268=1|269=0|", true, 270),
        ("W", "55=1|268=1|269=7|270=1|", false, 269),
        ("X", "268=1|279=1|269=0|278=1|55=1|270=1|", false, 279),
        ("X", "268=1|279=2|55=1|", true, 278),
        ("V", "262=a|263=3|264=1|", false, 263),
        ("V", "262=a|264=1|", true, 263),
        ("y", "320=a|322=b|560=0|146=1|1007=EURUSD|", false, 146),
        ("AP", "710=a|727=1|", true, 728),
        ("9", "11=a|39=0|434=3|", false, 434),
    ];
    for (kind, body, absent, tag) in cases {
        let msg_type = text(kind);
        let expected = if absent {
            FixError::MissingField { msg_type, tag }
        } else {
            FixError::InvalidField { msg_type, tag }
        };
        let result = received(&format!("35={kind}|{head}{body}"));
        assert_eq!(result, Err(expected), "{kind} {body}");
    }
    assert_eq!(
        received(&format!("35=ZZ|{head}58=x|")),
        Err(FixError::UnsupportedMessage(text("ZZ")))
    );
}

#[test]
fn order_identifier_of_another_form_is_read_as_absent() {
    let head = "34=1|49=CSERVER|52=20170117-08:03:04|56=me|";
    for value in ["0", "NONE", "-4"] {
        let report = received(&format!("35=8|{head}37={value}|150=8|39=8|")).expect("report");
        let Message::ExecutionReport(report) = report else {
            panic!("not an execution report");
        };
        assert_eq!(report.order_id, None, "{value}");
    }
}

#[test]
fn logon_never_shows_its_password_in_debug_output() {
    let logon = Message::from(Logon {
        encrypt_method: 0,
        heart_bt_int: 30,
        reset_seq_num: Some(true),
        username: Some(text("12345")),
        password: Some(FixPassword::new("passw0rd!").expect("password")),
    });
    let frame = logon.to_frame(header(1));
    for shown in [format!("{logon:?}"), format!("{frame:?}")] {
        assert!(!shown.contains("passw0rd!"), "{shown}");
        assert!(shown.contains("***"), "{shown}");
    }
    assert!(frame.body.contains(&Field::new(554, "passw0rd!")));
}

#[test]
fn only_session_level_messages_are_administrative() {
    let administrative: Vec<&str> = samples()
        .iter()
        .filter(|message| message.is_administrative())
        .map(Message::msg_type)
        .collect();
    assert_eq!(
        administrative,
        vec!["0", "0", "1", "2", "3", "4", "4", "5", "5", "A"]
    );
}

#[test]
fn malformed_bodies_never_panic_and_never_parse_in_part() {
    let garbage = [
        "",
        "x",
        "-1",
        "0",
        "99999999999999999999999",
        "1.5.5",
        "Y",
        "\u{fffd}",
    ];
    let mut parsed = 0_usize;
    let mut refused = 0_usize;
    for message in samples() {
        let kind = message.msg_type();
        let body = message.body();
        let mut variants = vec![Vec::new()];
        for index in 0..body.len() {
            let mut dropped = body.clone();
            dropped.remove(index);
            variants.push(dropped);
            let mut doubled = body.clone();
            doubled.extend(body.get(index).cloned());
            variants.push(doubled);
            variants.push(body.get(index..).expect("tail").to_vec());
            variants.push(body.get(..index).expect("head").to_vec());
            for value in garbage {
                let mut replaced = body.clone();
                if let Some(field) = replaced.get_mut(index) {
                    field.value = text(value);
                }
                variants.push(replaced);
            }
            for tag in [0, 55, 146, 267, 268, 269, 279, 702, u32::MAX] {
                let mut retagged = body.clone();
                if let Some(field) = retagged.get_mut(index) {
                    field.tag = tag;
                }
                variants.push(retagged);
            }
        }
        for variant in variants {
            match Message::parse(kind, &variant) {
                Ok(_) => parsed += 1,
                Err(
                    FixError::MissingField { msg_type, .. }
                    | FixError::InvalidField { msg_type, .. },
                ) => {
                    assert_eq!(msg_type, kind);
                    refused += 1;
                }
                Err(other) => panic!("unexpected error {other}"),
            }
        }
    }
    assert!(parsed > 100, "{parsed}");
    assert!(refused > 1_000, "{refused}");
}

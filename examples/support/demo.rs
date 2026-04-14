//! Guard that keeps the FIX examples on a demo account.
//!
//! The FIX sessions have no setting that names the environment; the account is named by the
//! sender identifier, which the server issues as `demo.<broker>.<login>` for a demo account
//! and as `live.<broker>.<login>` for a live one, and the hosts of the live environment
//! have a label of their name that starts with `live`.

/// Checks that the sender identifier and the host of a FIX session name a demo account.
///
/// # Errors
///
/// Returns what names another environment: a sender identifier that does not start with
/// `demo.`, or a host with a label that starts with `live`.
pub fn demo_only(sender_comp_id: &str, host: &str) -> Result<(), &'static str> {
    if !sender_comp_id.starts_with("demo.") {
        return Err("the sender identifier does not name a demo account");
    }
    let live = host
        .split('.')
        .any(|label| label.to_ascii_lowercase().starts_with("live"));
    if live {
        return Err("the host belongs to the live environment");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::demo_only;

    #[test]
    fn demo_sender_and_host_pass() {
        assert_eq!(
            demo_only("demo.broker.4242", "demo-uk-01.broker.example"),
            Ok(())
        );
        assert_eq!(demo_only("demo.broker.4242", "h51.broker.example"), Ok(()));
        assert_eq!(demo_only("demo.broker.4242", "127.0.0.1"), Ok(()));
        assert_eq!(demo_only("demo.broker.4242", "delivery.example"), Ok(()));
    }

    #[test]
    fn any_sender_that_is_not_a_demo_one_is_refused() {
        for sender in [
            "live.broker.4242",
            "LIVE.broker.4242",
            "broker.4242",
            "demo",
            "demobroker.4242",
            "Demo.broker.4242",
            " demo.broker.4242",
            "",
        ] {
            assert!(
                demo_only(sender, "demo-uk-01.broker.example").is_err(),
                "{sender}"
            );
        }
    }

    #[test]
    fn a_live_host_is_refused_whatever_the_sender() {
        for host in [
            "live-uk-01.broker.example",
            "LIVE-uk-01.broker.example",
            "live.broker.example",
            "h51.live.example",
        ] {
            assert!(demo_only("demo.broker.4242", host).is_err(), "{host}");
        }
    }
}

//! Guard that keeps the examples on a demo account.
//!
//! This module is the only place of the examples that reads a configuration or opens a
//! connection. A configuration leaves it wrapped in [`Checked`], which nothing else can
//! build, and a client is made from a [`Checked`] configuration only; a test scans the other
//! sources of the examples and fails when one of them builds a configuration or connects by
//! itself.
//!
//! The guard is an allowlist. An Open API configuration passes when its environment is demo
//! and its host is the demo host. A FIX configuration passes when its sender identifier
//! starts with `demo.`, one label of its host has the part `demo` and no label has a part
//! that starts with `live`; the parts of a label are separated by `-`, `demo` is matched in
//! lower case only and `live` in any case. Anything else is refused unless
//! [`ENV_ALLOW_LIVE`] permits it: `read-only` lets the examples that place no order leave
//! demo, `yes` lets every example leave it, and any other value permits nothing.

use std::fmt;

#[cfg(feature = "fix")]
use ctrader_connector::common::config::ConfigurationFix;
#[cfg(feature = "open-api")]
use ctrader_connector::common::config::ConfigurationOpenApi;
use ctrader_connector::common::config::{DEMO_HOST, Environment};
use ctrader_connector::common::errors::ConfigError;
#[cfg(any(feature = "fix", feature = "open-api"))]
use ctrader_connector::common::errors::ConnectorError;
#[cfg(feature = "fix")]
use ctrader_connector::fix::client::FixClient;
#[cfg(feature = "fix")]
use ctrader_connector::fix::session::SessionKind;
#[cfg(feature = "open-api")]
use ctrader_connector::open_api::client::OpenApiClient;

/// Variable that lets the examples leave the demo environment: `read-only` or `yes`.
pub const ENV_ALLOW_LIVE: &str = "BROKER_EXAMPLE_ALLOW_LIVE";

const PERMIT_READ_ONLY: &str = "read-only";
const PERMIT_ALL: &str = "yes";

/// What an example does with the account it reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// The example reads and places no order.
    ReadOnly,
    /// The example places orders.
    Trading,
}

/// Why the guard gave no configuration.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The environment holds no valid configuration.
    Config(ConfigError),
    /// The configuration names something that is not demo, and nothing permits it.
    NotDemo(&'static str),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "Config: {error}"),
            Self::NotDemo(reason) => write!(
                f,
                "the examples run against a demo account only: {reason}; see {ENV_ALLOW_LIVE}"
            ),
        }
    }
}

/// A configuration the guard let through.
///
/// Only this module builds one, so holding it proves the guard ran.
#[derive(Debug)]
pub struct Checked<C>(C);

impl<C> Checked<C> {
    /// Returns the configuration.
    pub const fn get(&self) -> &C {
        &self.0
    }
}

/// Tells whether the value of [`ENV_ALLOW_LIVE`] lets an example of `access` leave demo.
#[must_use]
pub fn permitted(value: Option<&str>, access: Access) -> bool {
    match value {
        Some(PERMIT_ALL) => true,
        Some(PERMIT_READ_ONLY) => access == Access::ReadOnly,
        _ => false,
    }
}

/// Checks that an Open API configuration targets the demo environment on the demo host.
///
/// # Errors
///
/// Returns what names something else: the environment or the host.
pub fn demo_open_api(environment: Environment, host: &str) -> Result<(), &'static str> {
    if environment != Environment::Demo {
        return Err("the environment is not demo");
    }
    if host != DEMO_HOST {
        return Err("the host is not the demo host");
    }
    Ok(())
}

/// Checks that the sender identifier and the host of a FIX session name a demo account.
///
/// The server issues the sender identifier of a demo account as `demo.<broker>.<login>`,
/// and the hosts of the demo environment carry `demo` in their name.
///
/// # Errors
///
/// Returns what names something else: a sender identifier that does not start with `demo.`,
/// a host without the part `demo`, or a host with a part that starts with `live`.
pub fn demo_fix(sender_comp_id: &str, host: &str) -> Result<(), &'static str> {
    if !sender_comp_id.starts_with("demo.") {
        return Err("the sender identifier does not name a demo account");
    }
    let parts = || host.split('.').flat_map(|label| label.split('-'));
    if parts().any(|part| part.to_ascii_lowercase().starts_with("live")) {
        return Err("the host belongs to the live environment");
    }
    if !parts().any(|part| part == "demo") {
        return Err("the host does not name the demo environment");
    }
    Ok(())
}

fn allowed(
    lookup: &impl Fn(&str) -> Option<String>,
    access: Access,
    demo: Result<(), &'static str>,
) -> Result<(), Refusal> {
    match demo {
        Ok(()) => Ok(()),
        Err(_) if permitted(lookup(ENV_ALLOW_LIVE).as_deref(), access) => Ok(()),
        Err(reason) => Err(Refusal::NotDemo(reason)),
    }
}

fn process(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Reads the Open API configuration from `lookup` and applies the guard to it.
///
/// # Errors
///
/// Returns a [`Refusal`] when a variable is absent or rejected, or the configuration is
/// not a demo one and `lookup` permits no other for `access`.
#[cfg(feature = "open-api")]
pub fn open_api_from(
    lookup: impl Fn(&str) -> Option<String>,
    access: Access,
) -> Result<Checked<ConfigurationOpenApi>, Refusal> {
    let configuration = ConfigurationOpenApi::from_lookup(&lookup).map_err(Refusal::Config)?;
    let demo = demo_open_api(configuration.environment(), configuration.host());
    allowed(&lookup, access, demo)?;
    Ok(Checked(configuration))
}

/// Reads the Open API configuration from the environment and applies the guard to it.
///
/// # Errors
///
/// Returns the refusals of [`open_api_from`].
#[cfg(feature = "open-api")]
pub fn open_api(access: Access) -> Result<Checked<ConfigurationOpenApi>, Refusal> {
    open_api_from(process, access)
}

/// Returns `checked` with a new token pair; the environment and the host stay the same.
///
/// # Errors
///
/// Returns a [`Refusal`] when a token is empty.
#[cfg(feature = "open-api")]
pub fn open_api_renewed(
    checked: &Checked<ConfigurationOpenApi>,
    access_token: &str,
    refresh_token: &str,
) -> Result<Checked<ConfigurationOpenApi>, Refusal> {
    let old = checked.get();
    let builder = match old.environment() {
        Environment::Demo => ConfigurationOpenApi::demo(),
        Environment::Live => ConfigurationOpenApi::live(),
    };
    builder
        .host(old.host())
        .port(old.port())
        .client_id(old.client_id().as_str())
        .client_secret(old.client_secret().expose())
        .access_token(access_token)
        .refresh_token(refresh_token)
        .token_url(old.token_url())
        .build()
        .map(Checked)
        .map_err(Refusal::Config)
}

/// Connects to the Open API with a configuration the guard let through.
///
/// # Errors
///
/// Returns the errors of [`OpenApiClient::connect`].
#[cfg(feature = "open-api")]
pub async fn open_api_client(
    checked: &Checked<ConfigurationOpenApi>,
) -> Result<OpenApiClient, ConnectorError> {
    OpenApiClient::connect(checked.0.clone()).await
}

/// Reads the FIX configuration from `lookup` and applies the guard to it.
///
/// # Errors
///
/// Returns a [`Refusal`] when a variable is absent or rejected, or the configuration is
/// not a demo one and `lookup` permits no other for `access`.
#[cfg(feature = "fix")]
pub fn fix_from(
    lookup: impl Fn(&str) -> Option<String>,
    access: Access,
) -> Result<Checked<ConfigurationFix>, Refusal> {
    let configuration = ConfigurationFix::from_lookup(&lookup).map_err(Refusal::Config)?;
    let demo = demo_fix(configuration.sender_comp_id(), configuration.host());
    allowed(&lookup, access, demo)?;
    Ok(Checked(configuration))
}

/// Reads the FIX configuration from the environment and applies the guard to it.
///
/// # Errors
///
/// Returns the refusals of [`fix_from`].
#[cfg(feature = "fix")]
pub fn fix(access: Access) -> Result<Checked<ConfigurationFix>, Refusal> {
    fix_from(process, access)
}

/// Logs on to a FIX session with a configuration the guard let through.
///
/// # Errors
///
/// Returns the errors of [`FixClient::connect`].
#[cfg(feature = "fix")]
pub async fn fix_client(
    checked: &Checked<ConfigurationFix>,
    kind: SessionKind,
) -> Result<FixClient, ConnectorError> {
    FixClient::connect(checked.0.clone(), kind).await
}

#[cfg(test)]
mod tests {
    use ctrader_connector::common::config::{DEMO_HOST, Environment, LIVE_HOST};

    use super::{Access, ENV_ALLOW_LIVE, Refusal, demo_fix, demo_open_api, permitted};

    const SENDER: &str = "demo.broker.4242";
    const HOST: &str = "demo-uk-01.broker.example";

    #[test]
    fn only_the_two_documented_values_permit_leaving_demo() {
        assert!(permitted(Some("yes"), Access::ReadOnly));
        assert!(permitted(Some("yes"), Access::Trading));
        assert!(permitted(Some("read-only"), Access::ReadOnly));
        assert!(!permitted(Some("read-only"), Access::Trading));
        for value in [
            "",
            " ",
            "1",
            "true",
            "YES",
            "Yes",
            " yes",
            "yes ",
            "y",
            "READ-ONLY",
            "trading",
        ] {
            for access in [Access::ReadOnly, Access::Trading] {
                assert!(!permitted(Some(value), access), "{value:?}");
            }
        }
        assert!(!permitted(None, Access::ReadOnly));
        assert!(!permitted(None, Access::Trading));
    }

    #[test]
    fn open_api_passes_only_the_demo_environment_on_the_demo_host() {
        assert_eq!(demo_open_api(Environment::Demo, DEMO_HOST), Ok(()));
        assert!(demo_open_api(Environment::Live, LIVE_HOST).is_err());
        assert!(demo_open_api(Environment::Live, DEMO_HOST).is_err());
        for host in [
            LIVE_HOST,
            "DEMO.ctraderapi.com",
            " demo.ctraderapi.com",
            "demo.ctraderapi.com ",
            "demo.ctraderapi.com.example",
            "127.0.0.1",
            "",
        ] {
            assert!(demo_open_api(Environment::Demo, host).is_err(), "{host:?}");
        }
    }

    #[test]
    fn fix_passes_a_demo_sender_on_a_host_that_names_demo() {
        for host in [
            HOST,
            "demo.broker.example",
            "h51.demo.example",
            "uk-demo-01.example",
        ] {
            assert_eq!(demo_fix(SENDER, host), Ok(()), "{host}");
        }
    }

    #[test]
    fn fix_refuses_any_sender_that_is_not_a_demo_one() {
        for sender in [
            "live.broker.4242",
            "LIVE.broker.4242",
            "broker.4242",
            "demo",
            "demobroker.4242",
            "DEMO.broker.4242",
            "Demo.broker.4242",
            " demo.broker.4242",
            "\tdemo.broker.4242",
            "",
        ] {
            assert!(demo_fix(sender, HOST).is_err(), "{sender:?}");
        }
    }

    #[test]
    fn fix_refuses_any_host_that_does_not_name_demo() {
        for host in [
            "h51.broker.example",
            "127.0.0.1",
            "eu-live-01.broker.example",
            "delivery.example",
            "demonstration.example",
            "DEMO.broker.example",
            "Demo-uk-01.broker.example",
            " demo.broker.example",
            "demo .broker.example",
            "",
        ] {
            assert!(demo_fix(SENDER, host).is_err(), "{host:?}");
        }
    }

    #[test]
    fn fix_refuses_a_live_host_even_when_it_names_demo() {
        for host in [
            "live-uk-01.broker.example",
            "LIVE-uk-01.broker.example",
            "demo.live.example",
            "demo-live-01.broker.example",
            "eu-live-01.demo.example",
            "demo.liveuk.example",
        ] {
            assert!(demo_fix(SENDER, host).is_err(), "{host}");
        }
    }

    #[test]
    fn a_refusal_names_the_variable_and_no_value() {
        let text = Refusal::NotDemo("the environment is not demo").to_string();
        assert!(text.contains(ENV_ALLOW_LIVE), "{text}");
        assert!(text.contains("demo account only"), "{text}");
    }

    fn lookup(pairs: Vec<(&'static str, &'static str)>) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .rev()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[cfg(feature = "open-api")]
    mod open_api {
        use ctrader_connector::common::config::{
            ConfigurationOpenApi, DEMO_HOST, ENV_ACCESS_TOKEN, ENV_CLIENT_ID, ENV_CLIENT_SECRET,
            ENV_DEMO_HOST, ENV_ENVIRONMENT, Environment, LIVE_HOST, RefreshToken,
        };
        use ctrader_connector::common::errors::ConfigError;

        use super::super::{Access, ENV_ALLOW_LIVE, Refusal, open_api_from, open_api_renewed};
        use super::lookup;

        const BASE: [(&str, &str); 3] = [
            (ENV_CLIENT_ID, "id"),
            (ENV_CLIENT_SECRET, "secret"),
            (ENV_ACCESS_TOKEN, "access"),
        ];

        fn with(extra: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> + use<> {
            lookup([&BASE[..], extra].concat())
        }

        #[test]
        fn a_demo_configuration_passes_for_every_access() {
            for access in [Access::ReadOnly, Access::Trading] {
                for extra in [
                    &[][..],
                    &[(ENV_ENVIRONMENT, "demo")][..],
                    &[(ENV_DEMO_HOST, DEMO_HOST)][..],
                ] {
                    let checked = open_api_from(with(extra), access);
                    let checked = checked.as_ref().map(super::super::Checked::get);
                    assert_eq!(
                        checked.map(ConfigurationOpenApi::environment),
                        Ok(Environment::Demo)
                    );
                    assert_eq!(checked.map(ConfigurationOpenApi::host), Ok(DEMO_HOST));
                }
            }
        }

        #[test]
        fn a_live_environment_is_refused_for_every_access() {
            for access in [Access::ReadOnly, Access::Trading] {
                for permit in [
                    &[][..],
                    &[(ENV_ALLOW_LIVE, "")][..],
                    &[(ENV_ALLOW_LIVE, "1")][..],
                    &[(ENV_ALLOW_LIVE, "YES")][..],
                ] {
                    let extra = [&[(ENV_ENVIRONMENT, "live")][..], permit].concat();
                    let refused = open_api_from(with(&extra), access).map(|_| ());
                    assert_eq!(
                        refused,
                        Err(Refusal::NotDemo("the environment is not demo"))
                    );
                }
            }
        }

        #[test]
        fn another_host_of_the_demo_environment_is_refused() {
            for host in ["live.ctraderapi.com", "127.0.0.1", "DEMO.ctraderapi.com"] {
                let refused =
                    open_api_from(with(&[(ENV_DEMO_HOST, host)]), Access::ReadOnly).map(|_| ());
                assert_eq!(
                    refused,
                    Err(Refusal::NotDemo("the host is not the demo host")),
                    "{host}"
                );
            }
        }

        #[test]
        fn the_read_only_permit_does_not_cover_trading() {
            let live = [(ENV_ENVIRONMENT, "live"), (ENV_ALLOW_LIVE, "read-only")];
            let reading = open_api_from(with(&live), Access::ReadOnly);
            assert_eq!(reading.as_ref().map(|c| c.get().host()), Ok(LIVE_HOST));
            let trading = open_api_from(with(&live), Access::Trading).map(|_| ());
            assert_eq!(
                trading,
                Err(Refusal::NotDemo("the environment is not demo"))
            );
        }

        #[test]
        fn the_full_permit_covers_trading() {
            let live = [(ENV_ENVIRONMENT, "live"), (ENV_ALLOW_LIVE, "yes")];
            for access in [Access::ReadOnly, Access::Trading] {
                let checked = open_api_from(with(&live), access);
                assert_eq!(
                    checked.as_ref().map(|c| c.get().environment()),
                    Ok(Environment::Live)
                );
            }
        }

        #[test]
        fn an_invalid_configuration_is_refused_as_such() {
            let refused = open_api_from(lookup(Vec::new()), Access::ReadOnly).map(|_| ());
            let expected = Refusal::Config(ConfigError::Missing(ENV_CLIENT_ID));
            assert_eq!(refused, Err(expected));
            let text = Refusal::Config(ConfigError::Missing(ENV_CLIENT_ID)).to_string();
            assert!(text.starts_with("Config: "), "{text}");
        }

        #[test]
        fn a_renewed_configuration_keeps_the_environment_and_the_host() {
            let live = [(ENV_ENVIRONMENT, "live"), (ENV_ALLOW_LIVE, "yes")];
            for (extra, environment, host) in [
                (&[][..], Environment::Demo, DEMO_HOST),
                (&live[..], Environment::Live, LIVE_HOST),
            ] {
                let old = open_api_from(with(extra), Access::ReadOnly);
                let new = old
                    .as_ref()
                    .map_err(|_| ())
                    .and_then(|old| open_api_renewed(old, "next", "after").map_err(|_| ()));
                let new = new.as_ref().map(super::super::Checked::get);
                assert_eq!(new.map(ConfigurationOpenApi::environment), Ok(environment));
                assert_eq!(new.map(ConfigurationOpenApi::host), Ok(host));
                assert_eq!(new.map(|c| c.access_token().expose()), Ok("next"));
                assert_eq!(
                    new.map(|c| c.refresh_token().map(RefreshToken::expose)),
                    Ok(Some("after"))
                );
            }
            let old = open_api_from(with(&[]), Access::ReadOnly);
            let empty = old
                .as_ref()
                .map_err(|_| ())
                .map(|old| open_api_renewed(old, "", "after").is_err());
            assert_eq!(empty, Ok(true));
        }
    }

    #[cfg(feature = "fix")]
    mod fix {
        use ctrader_connector::common::config::{
            ENV_FIX_HOST, ENV_FIX_PASSWORD, ENV_FIX_SENDER_COMP_ID, ENV_FIX_USERNAME,
        };
        use ctrader_connector::common::errors::ConfigError;

        use super::super::{Access, ENV_ALLOW_LIVE, Refusal, fix_from};
        use super::{HOST, SENDER, lookup};

        const BASE: [(&str, &str); 4] = [
            (ENV_FIX_HOST, HOST),
            (ENV_FIX_SENDER_COMP_ID, SENDER),
            (ENV_FIX_USERNAME, "4242"),
            (ENV_FIX_PASSWORD, "open-sesame"),
        ];

        fn with(extra: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> + use<> {
            lookup([&BASE[..], extra].concat())
        }

        #[test]
        fn a_demo_configuration_passes_for_every_access() {
            for access in [Access::ReadOnly, Access::Trading] {
                let checked = fix_from(with(&[]), access);
                assert_eq!(checked.as_ref().map(|c| c.get().host()), Ok(HOST));
            }
        }

        #[test]
        fn a_live_sender_or_host_is_refused_for_every_access() {
            for access in [Access::ReadOnly, Access::Trading] {
                for extra in [
                    [(ENV_FIX_SENDER_COMP_ID, "live.broker.4242")],
                    [(ENV_FIX_SENDER_COMP_ID, "DEMO.broker.4242")],
                    [(ENV_FIX_HOST, "eu-live-01.broker.example")],
                    [(ENV_FIX_HOST, "h51.broker.example")],
                    [(ENV_FIX_HOST, "127.0.0.1")],
                ] {
                    let refused = fix_from(with(&extra), access).map(|_| ());
                    assert!(
                        matches!(refused, Err(Refusal::NotDemo(_))),
                        "{:?}",
                        extra[0].1
                    );
                }
            }
        }

        #[test]
        fn the_read_only_permit_does_not_cover_trading() {
            let live = [
                (ENV_FIX_SENDER_COMP_ID, "live.broker.4242"),
                (ENV_FIX_HOST, "live-uk-01.broker.example"),
                (ENV_ALLOW_LIVE, "read-only"),
            ];
            assert!(fix_from(with(&live), Access::ReadOnly).is_ok());
            let trading = fix_from(with(&live), Access::Trading).map(|_| ());
            assert!(matches!(trading, Err(Refusal::NotDemo(_))));
        }

        #[test]
        fn the_full_permit_covers_trading() {
            let live = [
                (ENV_FIX_SENDER_COMP_ID, "live.broker.4242"),
                (ENV_FIX_HOST, "live-uk-01.broker.example"),
                (ENV_ALLOW_LIVE, "yes"),
            ];
            for access in [Access::ReadOnly, Access::Trading] {
                let checked = fix_from(with(&live), access);
                assert_eq!(
                    checked.as_ref().map(|c| c.get().sender_comp_id()),
                    Ok("live.broker.4242")
                );
            }
        }

        #[test]
        fn an_invalid_configuration_is_refused_as_such() {
            let refused = fix_from(lookup(Vec::new()), Access::ReadOnly).map(|_| ());
            assert_eq!(
                refused,
                Err(Refusal::Config(ConfigError::Missing(ENV_FIX_HOST)))
            );
        }
    }
}

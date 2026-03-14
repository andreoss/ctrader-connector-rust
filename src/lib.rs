//! Client library for the cTrader Open API and the cTrader FIX 4.4 API.
//!
//! Each protocol sits behind its own feature: `open-api` enables [`open_api`] and `fix` enables
//! [`fix`]. The [`common`] module is always available and holds the pieces shared by both.

pub mod common;

#[cfg(feature = "fix")]
pub mod fix;

#[cfg(feature = "open-api")]
pub mod open_api;

#[cfg(test)]
mod proto_text;

/// Version of this crate as recorded in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_matches_the_manifest() {
        let manifest = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        assert!(manifest.contains(&format!("version = \"{VERSION}\"")));
    }
}

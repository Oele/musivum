// Ported from LiveKit, pkg/clientconfiguration/match.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust, without the script interpreter (tengo) that the
// original compiles its expressions with. An expression is a Rust closure over the client's
// fields, written through `script_match!` with the text it has upstream, so `ScriptMatch::new`
// takes the closure next to the text and cannot fail, `Match` of a `ScriptMatch` cannot fail
// either, and there is no expression that is given as a string at run time. An expression
// that tengo refuses, when it compiles it (a name it does not know) or each time it runs it
// (an operator between types that do not have it, as `>` with the undefined value that a
// field which the client does not have is to it; a result that is not a boolean), does not
// compile here. `clientObject` and `ruleSdkVersion` are public, being the closure's
// parameter. What `IndexGet` gives name by name are the fields of `ClientObject`, all made at
// once by `ClientObject::new`; `BinaryOp` and `Equals` of `ruleSdkVersion` are
// `PartialOrd<&str>` and `PartialEq<&str>`, its `String()` is `Display`. `TypeName()` of both
// and `String()` of `clientObject` are left out: only tengo calls them. The client's
// information is a reference, which cannot be nil: the original panics on a nil one as soon
// as a rule reads a field. `strings.ToLower` is the function `to_lower` at the end.

use std::cmp::Ordering;
use std::fmt;

use super::{Error, semver};
use crate::protocol::livekit;
use crate::protocol::livekit::client_info::Sdk;

pub trait Match: Send + Sync {
    fn r#match(&self, client_info: &livekit::ClientInfo) -> Result<bool, Error>;
}

#[derive(Clone, Copy)]
pub struct ScriptMatch {
    // the expression as it is written in the source, for `Debug`
    expr: &'static str,
    compiled: fn(&ClientObject) -> bool,
}

impl ScriptMatch {
    /// `NewScriptMatch(expr)` of the original, for an expression that the Rust compiler has
    /// compiled: `expr` is its text, `compiled` what it does. Written as
    /// [`script_match!`](crate::script_match), which makes both of one text.
    pub fn new(expr: &'static str, compiled: fn(&ClientObject) -> bool) -> ScriptMatch {
        ScriptMatch { expr, compiled }
    }
}

/// `NewScriptMatch(expr)` of the original: `script_match!(|c| expr)` is the
/// [`ScriptMatch`](crate::clientconfiguration::ScriptMatch) of an expression over the client
/// `c`, with the text the expression has upstream.
///
/// ```
/// use musivum::clientconfiguration::Match;
/// use musivum::protocol::livekit::ClientInfo;
/// use musivum::script_match;
///
/// let m = script_match!(|c| c.protocol > 5 && c.browser != "firefox");
/// let client = ClientInfo {
///     protocol: 6,
///     browser: "Chrome".to_string(),
///     ..Default::default()
/// };
/// assert_eq!(Ok(true), m.r#match(&client));
/// ```
///
/// The expression is Rust, compiled with the rest, and gives what the original's script
/// gives because of what `c` is, a [`ClientObject`](crate::clientconfiguration::ClientObject):
///
/// - `c.sdk`, `c.os`, `c.device_model` and `c.browser` are strings in lower case,
///   `c.os_version` and `c.address` strings as the client sent them: `==` and `!=` with a
///   string compare them as they are. A field the client did not send is the empty string.
/// - `c.protocol` is a number.
/// - `c.version` and `c.browser_version` are versions
///   ([`RuleSdkVersion`](crate::clientconfiguration::RuleSdkVersion)): `<`, `<=`, `>`, `>=`,
///   `==` and `!=` with a string compare them as versions, see there.
/// - `&&`, `||` and parentheses are what they are in the script language, which leaves the
///   right side alone when the left one decides, as Rust does.
///
/// Nothing else compiles. Not a field that `c` does not have: in the original this
/// expression fails each time it runs, and no client gets what its rule says.
///
/// ```compile_fail,E0609
/// use musivum::script_match;
///
/// let m = script_match!(|c| c.protocols > 5);
/// ```
///
/// Not a name other than the client's, which the original's compiler refuses as well:
///
/// ```compile_fail,E0425
/// use musivum::script_match;
///
/// let m = script_match!(|c| cc.protocol > 5);
/// ```
///
/// And not an operator that the script language has no meaning for between two types (a
/// version on the right side of a comparison, a version compared with a version), nor one
/// that it has but no rule uses (`<` between strings, arithmetic).
#[macro_export]
macro_rules! script_match {
    (|$c:ident| $expr:expr) => {
        $crate::clientconfiguration::ScriptMatch::new(
            stringify!($expr),
            |$c: &$crate::clientconfiguration::ClientObject| -> bool { $expr },
        )
    };
}

// use result of eval script expression for match.
// expression examples:
// protocol bigger than 5 : c.protocol > 5
// browser if firefox: c.browser == "firefox"
// combined rule : c.protocol > 5 && c.browser == "firefox"
impl Match for ScriptMatch {
    fn r#match(&self, client_info: &livekit::ClientInfo) -> Result<bool, Error> {
        Ok((self.compiled)(&ClientObject::new(client_info)))
    }
}

impl fmt::Debug for ScriptMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ScriptMatch").field(&self.expr).finish()
    }
}

// ------------------------------------------------

/// The client as an expression sees it: the `c` of `c.browser == "firefox"`. The fields are
/// what the original's `IndexGet` gives for their names.
#[derive(Clone, Debug)]
pub struct ClientObject {
    /// The name the protocol has for the SDK, in lower case (`"js"`, `"react_native"`), and
    /// for a number it has no name for, the number (`"99"`).
    pub sdk: String,
    pub version: RuleSdkVersion,
    pub protocol: i64,
    pub os: String,
    pub os_version: String,
    pub device_model: String,
    pub browser: String,
    pub browser_version: RuleSdkVersion,
    pub address: String,
}

impl ClientObject {
    pub fn new(info: &livekit::ClientInfo) -> ClientObject {
        ClientObject {
            sdk: match Sdk::try_from(info.sdk) {
                Ok(sdk) => to_lower(sdk.as_str_name()),
                // the `String()` of a value without a name
                Err(_) => info.sdk.to_string(),
            },
            version: RuleSdkVersion {
                sdk_version: info.version.clone(),
            },
            protocol: i64::from(info.protocol),
            os: to_lower(&info.os),
            os_version: info.os_version.clone(),
            device_model: to_lower(&info.device_model),
            browser: to_lower(&info.browser),
            browser_version: RuleSdkVersion {
                sdk_version: info.browser_version.clone(),
            },
            address: info.address.clone(),
        }
    }
}

// ------------------------------------------

/// A version of an SDK or of a browser, as a client sent it. Compared with a string, it is
/// earlier, the same or later as a semantic version when both are one in the form that Go's
/// `semver` package takes behind a "v" ("18", "18.3", "18.3.1", "2.17.1-rc.1"), so that
/// "18.10" is later than "18.3" and "18.3.0" the same as it. When either is not (no version
/// at all, four numbers as in "120.0.6099.129", a zero in front as in "18.03", a space) the
/// two are compared as strings, byte by byte: "9.1.2.3" is later than "18.3" and
/// "100.0.0.0" earlier.
#[derive(Clone, Debug)]
pub struct RuleSdkVersion {
    sdk_version: String,
}

impl fmt::Display for RuleSdkVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.sdk_version)
    }
}

impl PartialOrd<&str> for RuleSdkVersion {
    fn partial_cmp(&self, rhs: &&str) -> Option<Ordering> {
        Some(self.compare(rhs).cmp(&0))
    }
}

impl PartialEq<&str> for RuleSdkVersion {
    fn eq(&self, rhs: &&str) -> bool {
        self.compare(rhs) == 0
    }
}

impl RuleSdkVersion {
    fn compare(&self, rhs_sdk_version: &str) -> i64 {
        if !semver::is_valid(&format!("v{}", self.sdk_version))
            || !semver::is_valid(&format!("v{rhs_sdk_version}"))
        {
            // if not valid semver, do string compare
            if self.sdk_version.as_str() < rhs_sdk_version {
                return -1;
            } else if self.sdk_version.as_str() > rhs_sdk_version {
                return 1;
            }
        } else {
            return semver::compare(
                &format!("v{}", self.sdk_version),
                &format!("v{rhs_sdk_version}"),
            );
        }
        0
    }
}

// Go's `strings.ToLower`, which puts one character in the place of each. (Rust's
// `to_lowercase` makes two characters of 'İ' (U+0130), of which Go keeps the 'i': a browser
// that calls itself "FİREFOX" is "firefox" to the original's rules, and to these.)
fn to_lower(s: &str) -> String {
    s.chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect()
}

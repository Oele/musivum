//! What the server makes of a client's `User-Agent` header: browser, operating system and
//! device, in the words of the parser LiveKit uses. Written new for musivum.
//!
//! LiveKit's `AugmentClientInfo` (`utils.rs`) parses the header with `ua-parser/uap-go` and
//! the regular expressions of `uap-core` (about 1,500 of them), plus one rule of its own for
//! OBS Studio. What the rest of the server reads of the result is little: the per-client
//! rules (`clientconfiguration`) match the browser `safari`, `firefox` and `firefox mobile`,
//! a Safari version and the operating system `android`; `rtc/clientinfo.rs` asks for Firefox,
//! Safari, Linux, Android and OBS. So this is a recogniser, not a port of that parser: for
//! the browsers, systems and devices it knows, it gives what `uap-core` gives, word for word
//! and with the same versions (held against a run of the original by `useragent_test`); for
//! anything else it gives `Other`, where `uap-core` may know a name.
//!
//! Known: the browsers Chrome, Chrome Mobile, Chrome Mobile WebView, Chrome Mobile iOS,
//! HeadlessChrome, Firefox, Firefox Mobile, Firefox iOS, Safari, Mobile Safari, Mobile Safari
//! UI/WKWebView, Edge, Edge Mobile, Opera, Samsung Internet, Electron, okhttp (React Native
//! on Android) and OBS Studio; the systems Windows (NT 6.1 to 10.0), Mac OS X, iOS, Android,
//! Chrome OS, Ubuntu and Linux; the devices iPhone, iPad, Mac, and of Android the reduced
//! `K`, Samsung's `SM-` models, the generic phone and tablet of Firefox, and otherwise the
//! model as the browser writes it (`uap-core` puts the brand before many of those, `XiaoMi
//! Redmi Note 9` for `Redmi Note 9`; that is not done here).

/// uap-go's `UserAgent`: the browser.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserAgent {
    pub family: String,
    pub major: String,
    pub minor: String,
    pub patch: String,
}

impl UserAgent {
    /// uap-go's `ToVersionString`: the parts there are, joined with dots.
    pub fn to_version_string(&self) -> String {
        join_version(&[&self.major, &self.minor, &self.patch])
    }
}

/// uap-go's `Os`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Os {
    pub family: String,
    pub major: String,
    pub minor: String,
    pub patch: String,
    pub patch_minor: String,
}

impl Os {
    /// uap-go's `ToVersionString`.
    pub fn to_version_string(&self) -> String {
        join_version(&[&self.major, &self.minor, &self.patch, &self.patch_minor])
    }
}

/// uap-go's `Device`, without the brand, which LiveKit does not read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Device {
    pub family: String,
    pub model: String,
}

/// uap-go's `Client`: what a user agent string says.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Client {
    pub user_agent: UserAgent,
    pub os: Os,
    pub device: Device,
}

const OTHER: &str = "Other";

/// uap-go's `Parser.Parse` for what this recogniser knows (see the module).
pub fn parse_user_agent(line: &str) -> Client {
    Client {
        user_agent: parse_browser(line),
        os: parse_os(line),
        device: parse_device(line),
    }
}

fn join_version(parts: &[&String]) -> String {
    let mut version = String::new();
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i > 0 {
            version.push('.');
        }
        version.push_str(part);
    }
    version
}

// The version after `token` (`Chrome/` and the like): up to three numbers separated by dots,
// as the expressions of uap-core take them.
fn version_after<'a>(line: &'a str, token: &str) -> Option<(&'a str, Vec<&'a str>)> {
    let start = line.find(token)? + token.len();
    let rest = &line[start..];
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(rest.len());
    let parts: Vec<&str> = rest[..end].split('.').take(3).collect();
    if parts.first().is_none_or(|major| major.is_empty()) {
        return None;
    }
    Some((&rest[end..], parts))
}

fn user_agent(family: &str, parts: &[&str]) -> UserAgent {
    let part = |i: usize| parts.get(i).map(|p| p.to_string()).unwrap_or_default();
    UserAgent {
        family: family.to_string(),
        major: part(0),
        minor: part(1),
        patch: part(2),
    }
}

fn is_ios_device(line: &str) -> bool {
    line.contains("iPhone") || line.contains("iPad") || line.contains("iPod")
}

fn parse_browser(line: &str) -> UserAgent {
    let android = line.contains("Android");
    // in the order in which uap-core's expressions find them
    for (token, family) in [
        ("Electron/", "Electron"),
        ("EdgA/", "Edge Mobile"),
        ("Edg/", "Edge"),
        ("OPR/", "Opera"),
        ("SamsungBrowser/", "Samsung Internet"),
        ("HeadlessChrome/", "HeadlessChrome"),
        ("CriOS/", "Chrome Mobile iOS"),
        ("FxiOS/", "Firefox iOS"),
    ] {
        if let Some((_, parts)) = version_after(line, token) {
            return user_agent(family, &parts);
        }
    }
    if let Some((rest, parts)) = version_after(line, "Chrome/") {
        if android && line.contains("; wv)") {
            return user_agent("Chrome Mobile WebView", &parts);
        }
        // `Chrome/1.2.3.4 Mobile`
        if let Some(after) = rest.strip_prefix(" Mobile")
            && (after.is_empty() || after.starts_with([' ', '/']))
        {
            return user_agent("Chrome Mobile", &parts);
        }
        return user_agent("Chrome", &parts);
    }
    if let Some((_, parts)) = version_after(line, "Firefox/") {
        let before = &line[..line.find("Firefox/").unwrap_or(0)];
        if before.contains("Mobile;") || before.contains("Tablet;") {
            return user_agent("Firefox Mobile", &parts);
        }
        return user_agent("Firefox", &parts);
    }
    if is_ios_device(line)
        && line.contains("Safari")
        && let Some((_, parts)) = version_after(line, "Version/")
    {
        return user_agent("Mobile Safari", &parts);
    }
    if is_ios_device(line) && line.contains("AppleWebKit") && !line.contains("Safari") {
        return user_agent("Mobile Safari UI/WKWebView", &[]);
    }
    if line.contains("Safari/")
        && let Some((_, parts)) = version_after(line, "Version/")
    {
        return user_agent("Safari", &parts);
    }
    if let Some((_, parts)) = version_after(line, "okhttp/") {
        return user_agent("okhttp", &parts);
    }
    // LiveKit's own rule, after all others
    if let Some((_, parts)) = version_after(line, "OBS-Studio/") {
        return user_agent("OBS Studio", &parts);
    }
    user_agent(OTHER, &[])
}

// The numbers after `token`, separated by `_` or `.`, at most four.
fn os_version_after(line: &str, token: &str) -> Option<Vec<String>> {
    let start = line.find(token)? + token.len();
    let rest = &line[start..];
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_'))
        .unwrap_or(rest.len());
    let parts: Vec<String> = rest[..end]
        .split(['.', '_'])
        .take(4)
        .map(str::to_string)
        .collect();
    if parts.first().is_none_or(|major| major.is_empty()) {
        return None;
    }
    Some(parts)
}

fn os(family: &str, parts: &[String]) -> Os {
    let part = |i: usize| parts.get(i).cloned().unwrap_or_default();
    Os {
        family: family.to_string(),
        major: part(0),
        minor: part(1),
        patch: part(2),
        patch_minor: part(3),
    }
}

fn parse_os(line: &str) -> Os {
    if let Some(start) = line.find("Windows NT ") {
        let version = &line[start + "Windows NT ".len()..];
        let parts: &[&str] = if version.starts_with("10.0") {
            &["10"]
        } else if version.starts_with("6.3") {
            &["8", "1"]
        } else if version.starts_with("6.2") {
            &["8"]
        } else if version.starts_with("6.1") {
            &["7"]
        } else {
            &[]
        };
        let parts: Vec<String> = parts.iter().map(|p| p.to_string()).collect();
        return os("Windows", &parts);
    }
    if line.contains("Windows") {
        return os("Windows", &[]);
    }
    if is_ios_device(line)
        && let Some(parts) = os_version_after(line, " OS ")
    {
        // iOS writes up to three numbers
        return os("iOS", &parts[..parts.len().min(3)]);
    }
    if let Some(parts) = os_version_after(line, "Android ") {
        return os("Android", &parts[..parts.len().min(3)]);
    }
    if let Some(start) = line.find("CrOS ") {
        // `CrOS x86_64 14541.0.0`: the version follows the architecture
        let rest = &line[start + "CrOS ".len()..];
        if let Some(space) = rest.find(' ')
            && let Some(parts) = os_version_after(&rest[space..], " ")
        {
            return os("Chrome OS", &parts[..parts.len().min(3)]);
        }
        return os("Chrome OS", &[]);
    }
    if let Some(parts) = os_version_after(line, "Mac OS X ") {
        return os("Mac OS X", &parts[..parts.len().min(3)]);
    }
    if line.contains("Ubuntu") {
        return os("Ubuntu", &[]);
    }
    if line.contains("Linux") {
        return os("Linux", &[]);
    }
    os(OTHER, &[])
}

fn device(family: &str, model: &str) -> Device {
    Device {
        family: family.to_string(),
        model: model.to_string(),
    }
}

fn parse_device(line: &str) -> Device {
    if line.contains("iPhone") {
        return device("iPhone", "iPhone");
    }
    if line.contains("iPad") {
        return device("iPad", "iPad");
    }
    if line.contains("Macintosh") {
        return device("Mac", "Mac");
    }
    // Firefox for Android: `(Android 14; Mobile; rv:133.0)`
    if line.contains("(Android ") {
        if line.contains("; Mobile;") {
            return device("Generic Smartphone", "Smartphone");
        }
        if line.contains("; Tablet;") {
            return device("Generic Tablet", "Tablet");
        }
    }
    // the others: `(Linux; Android 14; Pixel 8 Build/AP2A.240805.005; wv)`
    if let Some(start) = line.find("Linux; Android ") {
        let rest = &line[start + "Linux; Android ".len()..];
        let end = rest.find(')').unwrap_or(rest.len());
        let mut segment = &rest[..end];
        // after the version
        let Some(semicolon) = segment.find("; ") else {
            return device(OTHER, "");
        };
        segment = &segment[semicolon + 2..];
        let web_view = segment.ends_with("; wv");
        if let Some(model) = segment.strip_suffix("; wv") {
            segment = model;
        }
        if let Some(build) = segment.find(" Build/") {
            segment = &segment[..build];
        }
        let model = segment.trim();
        if model == "K" {
            if web_view {
                return device("Generic Smartphone", "Smartphone");
            }
            return device("K", "K");
        }
        let samsung = model.strip_prefix("SAMSUNG ").unwrap_or(model);
        if samsung.starts_with("SM-") {
            return device(&format!("Samsung {samsung}"), samsung);
        }
        if !model.is_empty() {
            return device(model, model);
        }
    }
    device(OTHER, "")
}

#[cfg(test)]
#[path = "useragent_test.rs"]
mod useragent_test;

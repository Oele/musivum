// New for musivum: what `augment_client_info` makes of user agents of current browsers,
// against what LiveKit's server (uap-go with uap-core's expressions) made of the same strings
// in a run of the original at v1.13.11: browser, browser version, operating system, its
// version, device model.

use http::Request;

use crate::protocol::livekit;
use crate::service::augment_client_info;

#[rustfmt::skip]
const CASES: &[(&str, [&str; 5])] = &[
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Windows", "10", "Other"],
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Mac OS X", "10.15.7", "Mac"],
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Linux", "", "Other"],
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/157.0.0.0 Safari/537.36",
            ["HeadlessChrome", "157.0.0", "Linux", "", "Other"],
        ),
        (
            "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Chrome OS", "14541.0.0", "Other"],
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
            ["Firefox", "133.0", "Linux", "", "Other"],
        ),
        (
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
            ["Firefox", "133.0", "Ubuntu", "", "Other"],
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
            ["Firefox", "133.0", "Windows", "10", "Other"],
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
            ["Firefox", "133.0", "Mac OS X", "10.15", "Mac"],
        ),
        (
            "Mozilla/5.0 (Android 14; Mobile; rv:133.0) Gecko/133.0 Firefox/133.0",
            ["Firefox Mobile", "133.0", "Android", "14", "Generic Smartphone Smartphone"],
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
            ["Safari", "18.1", "Mac OS X", "10.15.7", "Mac"],
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3.1 Safari/605.1.15",
            ["Safari", "18.3.1", "Mac OS X", "10.15.7", "Mac"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
            ["Mobile Safari", "18.1", "iOS", "18.1", "iPhone"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Mobile/15E148 Safari/604.1",
            ["Mobile Safari", "18.1.1", "iOS", "18.1.1", "iPhone"],
        ),
        (
            "Mozilla/5.0 (iPad; CPU OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
            ["Mobile Safari", "17.6", "iOS", "17.6", "iPad"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/131.0.6778.73 Mobile/15E148 Safari/604.1",
            ["Chrome Mobile iOS", "131.0.6778", "iOS", "18.1", "iPhone"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/133.0 Mobile/15E148 Safari/605.1.15",
            ["Firefox iOS", "133.0", "iOS", "18.1", "iPhone"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
            ["Mobile Safari UI/WKWebView", "", "iOS", "18.1", "iPhone"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            ["Chrome Mobile", "131.0.0", "Android", "10", "K"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/AP2A.240805.005; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/131.0.6778.135 Mobile Safari/537.36",
            ["Chrome Mobile WebView", "131.0.6778", "Android", "14", "Pixel 8"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/131.0.6778.135 Mobile Safari/537.36",
            ["Chrome Mobile WebView", "131.0.6778", "Android", "10", "Generic Smartphone Smartphone"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 12; 2201117TI) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            ["Chrome Mobile", "120.0.0", "Android", "12", "2201117TI"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 14; SAMSUNG SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/27.0 Chrome/125.0.0.0 Mobile Safari/537.36",
            ["Samsung Internet", "27.0", "Android", "14", "Samsung SM-S918B SM-S918B"],
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            ["Edge", "131.0.0", "Windows", "10", "Other"],
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/115.0.0.0",
            ["Opera", "115.0.0", "Windows", "10", "Other"],
        ),
        (
            "Mozilla/5.0 (OBS-Studio/30.1.2; Windows x86_64; en-US)",
            ["OBS Studio", "30.1.2", "Windows", "", "Other"],
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Element/1.11.85 Chrome/130.0.6723.137 Electron/33.2.1 Safari/537.36",
            ["Electron", "33.2.1", "Windows", "10", "Other"],
        ),
        (
            "Dart/3.5 (dart:io)",
            ["Other", "", "Other", "", "Other"],
        ),
        (
            "okhttp/4.12.0",
            ["okhttp", "4.12.0", "Other", "", "Other"],
        ),
        (
            "",
            ["Other", "", "Other", "", "Other"],
        ),
        (
            "Mozilla/5.0 (Android 14; Tablet; rv:133.0) Gecko/133.0 Firefox/133.0",
            ["Firefox Mobile", "133.0", "Android", "14", "Generic Tablet Tablet"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 14; Pixel Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Android", "14", "Pixel Tablet"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ["Chrome", "131.0.0", "Android", "10", "K"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/131.0.6778.135 Safari/537.36",
            ["Chrome Mobile WebView", "131.0.6778", "Android", "10", "Generic Smartphone Smartphone"],
        ),
        (
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            ["Chrome", "109.0.0", "Windows", "7", "Other"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 13; SM-A536B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            ["Chrome Mobile", "131.0.0", "Android", "13", "Samsung SM-A536B SM-A536B"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 4.4.2; Nexus 5 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36",
            ["Chrome Mobile", "34.0.1847", "Android", "4.4.2", "Nexus 5"],
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.2903.86",
            ["Edge", "131.0.2903", "Mac OS X", "10.15.7", "Mac"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36 EdgA/131.0.0.0",
            ["Edge Mobile", "131.0.0", "Android", "10", "K"],
        ),
        (
            "Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/AP2A.240805.005) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            ["Chrome Mobile", "131.0.0", "Android", "14", "Pixel 8"],
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1 OBS-Studio/30.0",
            ["Mobile Safari", "18.1", "iOS", "18.1", "iPhone"],
        ),
];

#[test]
fn test_user_agents_as_upstream_names_them() {
    for (ua, want) in CASES {
        let r = Request::get("/rtc")
            .header("User-Agent", *ua)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut ci = livekit::ClientInfo {
            sdk: livekit::client_info::Sdk::Js as i32,
            ..Default::default()
        };
        augment_client_info(&mut ci, &r);
        let got = [
            ci.browser.as_str(),
            ci.browser_version.as_str(),
            ci.os.as_str(),
            ci.os_version.as_str(),
            ci.device_model.as_str(),
        ];
        assert_eq!(*want, got, "{ua}");
    }
}

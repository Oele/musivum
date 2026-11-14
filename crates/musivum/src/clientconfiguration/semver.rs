//! What the Go package `golang.org/x/mod/semver` says about version strings, as far as the
//! rules of this package ask it: whether a string is a semantic version, and which of two is
//! the later one. Written new for musivum from Semantic Versioning 2.0.0 and from the two
//! things that package's documentation says it does differently: a version starts with "v",
//! and "v1" and "v1.2", with nothing behind them, are short for "v1.0.0" and "v1.2.0". The
//! reference test holds it against the original.

use std::cmp::Ordering;

// A valid version in its parts. The numbers are kept as they are written, which is without
// zeros in front: a version may have numbers that fit no integer type.
struct Version<'a> {
    major: &'a str,
    minor: &'a str,
    patch: &'a str,
    // without the hyphen; `None` for a release
    prerelease: Option<&'a str>,
}

/// Go's `semver.IsValid(v)`.
pub(super) fn is_valid(v: &str) -> bool {
    parse(v).is_some()
}

/// Go's `semver.Compare(v, w)`: -1, 0 or +1 for `v` being earlier than, the same as or later
/// than `w`. A string that is not a version is earlier than every version and the same as
/// every other such string. Build metadata ("+...") makes no difference.
pub(super) fn compare(v: &str, w: &str) -> i64 {
    let ordering = match (parse(v), parse(w)) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(v), Some(w)) => compare_numbers(v.major, w.major)
            .then_with(|| compare_numbers(v.minor, w.minor))
            .then_with(|| compare_numbers(v.patch, w.patch))
            .then_with(|| compare_prereleases(v.prerelease, w.prerelease)),
    };
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn parse(v: &str) -> Option<Version<'_>> {
    let (major, rest) = number(v.strip_prefix('v')?)?;
    if rest.is_empty() {
        return Some(Version {
            major,
            minor: "0",
            patch: "0",
            prerelease: None,
        });
    }
    let (minor, rest) = number(rest.strip_prefix('.')?)?;
    if rest.is_empty() {
        return Some(Version {
            major,
            minor,
            patch: "0",
            prerelease: None,
        });
    }
    let (patch, rest) = number(rest.strip_prefix('.')?)?;

    // Behind the three numbers: a pre-release behind a hyphen, then build metadata behind a
    // plus sign, each a list of identifiers with dots between them.
    let (rest, build) = match rest.split_once('+') {
        Some((rest, build)) => (rest, Some(build)),
        None => (rest, None),
    };
    let prerelease = if rest.is_empty() {
        None
    } else {
        Some(rest.strip_prefix('-')?)
    };
    // "Numeric identifiers MUST NOT include leading zeroes."
    let prerelease_is_valid = prerelease.is_none_or(|prerelease| {
        prerelease
            .split('.')
            .all(|id| is_identifier(id) && !(is_numeric(id) && id.len() > 1 && id.starts_with('0')))
    });
    let build_is_valid = build.is_none_or(|build| build.split('.').all(is_identifier));
    if !prerelease_is_valid || !build_is_valid {
        return None;
    }
    Some(Version {
        major,
        minor,
        patch,
        prerelease,
    })
}

// The number that `v` starts with, and what follows it: digits, without a zero in front of
// other digits.
fn number(v: &str) -> Option<(&str, &str)> {
    let end = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    let (number, rest) = v.split_at(end);
    if number.is_empty() || (number.len() > 1 && number.starts_with('0')) {
        return None;
    }
    Some((number, rest))
}

// "Identifiers MUST comprise only ASCII alphanumerics and hyphens [0-9A-Za-z-]. Identifiers
// MUST NOT be empty."
fn is_identifier(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn is_numeric(id: &str) -> bool {
    id.chars().all(|c| c.is_ascii_digit())
}

// Two numbers without zeros in front: the one with more digits is the larger one.
fn compare_numbers(x: &str, y: &str) -> Ordering {
    x.len().cmp(&y.len()).then_with(|| x.cmp(y))
}

// "When major, minor, and patch are equal, a pre-release version has lower precedence than a
// normal version." Between two pre-releases the first identifier that differs decides:
// "identifiers consisting of only digits are compared numerically", those "with letters or
// hyphens are compared lexically in ASCII sort order", and "numeric identifiers always have
// lower precedence than non-numeric identifiers". "A larger set of pre-release fields has a
// higher precedence than a smaller set, if all of the preceding identifiers are equal."
fn compare_prereleases(x: Option<&str>, y: Option<&str>) -> Ordering {
    let (x, y) = match (x, y) {
        (None, None) => return Ordering::Equal,
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
        (Some(x), Some(y)) => (x, y),
    };
    let (mut xs, mut ys) = (x.split('.'), y.split('.'));
    loop {
        let (x, y) = match (xs.next(), ys.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(x), Some(y)) => (x, y),
        };
        let ordering = match (is_numeric(x), is_numeric(y)) {
            (true, true) => compare_numbers(x, y),
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => x.cmp(y),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The order that Semantic Versioning 2.0.0 gives as its example (section 11).
    #[test]
    fn the_specification_s_example_is_in_order() {
        let versions = [
            "v1.0.0-alpha",
            "v1.0.0-alpha.1",
            "v1.0.0-alpha.beta",
            "v1.0.0-beta",
            "v1.0.0-beta.2",
            "v1.0.0-beta.11",
            "v1.0.0-rc.1",
            "v1.0.0",
        ];
        for (i, v) in versions.iter().enumerate() {
            assert!(is_valid(v), "{v}");
            for (j, w) in versions.iter().enumerate() {
                let expected = match i.cmp(&j) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                };
                assert_eq!(expected, compare(v, w), "{v} and {w}");
            }
        }
    }

    #[test]
    fn the_short_forms_and_what_is_no_version() {
        for v in ["v1", "v1.2", "v1.2.3", "v0.0.0", "v1.2.3-a.b-c.0+x.007"] {
            assert!(is_valid(v), "{v}");
        }
        for v in [
            "",
            "v",
            "1.2.3",
            "v01",
            "v1.02",
            "v1.",
            "v1.2.",
            "v1-a",
            "v1.2-a",
            "v1.2+b",
            "v1.2.3-",
            "v1.2.3-a..b",
            "v1.2.3-01",
            "v1.2.3+",
            "v1.2.3+a+b",
            "v1.2.3 ",
            "v1.2.3.4",
        ] {
            assert!(!is_valid(v), "{v}");
        }
        assert_eq!(0, compare("v1", "v1.0.0"));
        assert_eq!(0, compare("v1.2", "v1.2.0+build"));
        assert_eq!(1, compare("v1.10", "v1.9.9"));
        assert_eq!(-1, compare("v9", "v10"));
        assert_eq!(0, compare("no version", ""));
        assert_eq!(-1, compare("no version", "v0"));
        assert_eq!(1, compare("v0", "no version"));
    }
}

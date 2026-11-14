//! Prometheus' text exposition format, version 0.0.4: what a scrape of the metrics endpoint
//! reads. Written new for musivum, after what the Go client that LiveKit is built with
//! writes (`prometheus/client_golang` v1.24.1 with `prometheus/common` v0.72.0, both
//! Apache-2.0): its registry's `Gather` and `expfmt`'s `MetricFamilyToText` for the order
//! and the text, `expfmt.Negotiate` with goautoneg's `ParseAccept` (BSD-3-Clause) for the
//! content type. The test holds the output against that client's for the same collectors
//! and values (`testdata/exposition_test.go`).
//!
//! What the Go client does that is followed here:
//! - Families are written in the order of their names, and a family without samples (a
//!   vector whose label values were never used) is left out.
//! - The labels of a sample are written in the order of their names, the constant labels
//!   among them, and the samples of a family in the order of their label values taken in
//!   that order (bytewise, as Go compares strings).
//! - `# HELP` is written for every family, also with an empty text (the Go client always has
//!   one); backslash and newline are escaped in it, and in label values the double quote too.
//! - Numbers are written as Go's `strconv.FormatFloat(f, 'g', -1, 64)` writes them (shortest
//!   digits, an exponent from 1e+06 up and below 1e-04), with `+Inf`, `-Inf` and `NaN`; a
//!   counter is written as the float the Go client keeps it as.
//! - A histogram is its cumulative `_bucket` samples with `le`, one for `+Inf`, `_sum` and
//!   `_count`.
//!
//! Metric and label names are taken to be valid as the format has always had them
//! (`[a-zA-Z_:][a-zA-Z0-9_:]*`): they are written as they are. The names of this package
//! are. So is every escaping scheme the content type can name the same for them, which is
//! why only the text of the content type depends on it.

use std::cmp::Ordering;
use std::fmt::Write;

use super::collectors::{MetricFamily, Value};

/// The content type of the text format, before the escaping scheme: expfmt's `FmtText`.
pub const FMT_TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";

// What expfmt adds to a content type when the request does not ask for another scheme
// (model.NameEscapingScheme).
const DEFAULT_ESCAPING_SCHEME: &str = "underscores";

impl MetricFamily {
    /// The family with `labels` added to every sample: what upstream's collectors have as
    /// `ConstLabels` (`node_id` and `node_type`).
    pub fn with_const_labels(mut self, labels: &[(&'static str, &str)]) -> Self {
        for (name, value) in labels {
            self.label_names.push(name);
            for sample in &mut self.samples {
                sample.label_values.push(value.to_string());
            }
        }
        self
    }
}

/// `families` in the text format, as the Go client's registry gathers them and its handler
/// writes them.
pub fn encode_text(families: &[MetricFamily]) -> String {
    let mut sorted: Vec<&MetricFamily> = families
        .iter()
        .filter(|family| !family.samples.is_empty())
        .collect();
    sorted.sort_by(|a, b| a.name.cmp(b.name));
    let mut out = String::new();
    for family in sorted {
        metric_family_to_text(&mut out, family);
    }
    out
}

/// One family in the text format. A family without samples gives nothing (the Go client
/// gathers no such family).
pub fn metric_family_to_text(out: &mut String, family: &MetricFamily) {
    let Some(first) = family.samples.first() else {
        return;
    };
    let name = family.name;

    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    write_escaped_string(out, family.help, false);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(match first.value {
        Value::Counter(_) => " counter\n",
        Value::Gauge(_) => " gauge\n",
        Value::Histogram(_) => " histogram\n",
    });

    // the labels of every sample in the order of their names
    let mut order: Vec<usize> = (0..family.label_names.len()).collect();
    order.sort_by_key(|&i| family.label_names[i]);
    let mut samples: Vec<(Vec<(&str, &str)>, &Value)> = family
        .samples
        .iter()
        .map(|sample| {
            let labels = order
                .iter()
                .map(|&i| (family.label_names[i], sample.label_values[i].as_str()))
                .collect();
            (labels, &sample.value)
        })
        .collect();
    samples.sort_by(|(a, _), (b, _)| compare_label_values(a, b));

    for (labels, value) in samples {
        match value {
            Value::Counter(v) => write_sample(out, name, "", &labels, None, *v as f64),
            Value::Gauge(v) => write_sample(out, name, "", &labels, None, *v),
            Value::Histogram(snapshot) => {
                let mut inf_seen = false;
                for (upper_bound, cumulative_count) in &snapshot.buckets {
                    write_sample(
                        out,
                        name,
                        "_bucket",
                        &labels,
                        Some(("le", *upper_bound)),
                        *cumulative_count as f64,
                    );
                    if *upper_bound == f64::INFINITY {
                        inf_seen = true;
                    }
                }
                if !inf_seen {
                    write_sample(
                        out,
                        name,
                        "_bucket",
                        &labels,
                        Some(("le", f64::INFINITY)),
                        snapshot.count as f64,
                    );
                }
                write_sample(out, name, "_sum", &labels, None, snapshot.sum);
                write_sample(out, name, "_count", &labels, None, snapshot.count as f64);
            }
        }
    }
}

// The Go client's order of the metrics of a family: fewer labels first, then by the values
// of the labels, which are in the order of their names.
fn compare_label_values(a: &[(&str, &str)], b: &[(&str, &str)]) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| {
        a.iter()
            .zip(b)
            .map(|((_, va), (_, vb))| va.cmp(vb))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal)
    })
}

fn write_sample(
    out: &mut String,
    name: &str,
    suffix: &str,
    labels: &[(&str, &str)],
    additional_label: Option<(&str, f64)>,
    value: f64,
) {
    out.push_str(name);
    out.push_str(suffix);
    if !labels.is_empty() || additional_label.is_some() {
        let mut separator = '{';
        for (label_name, label_value) in labels {
            out.push(separator);
            out.push_str(label_name);
            out.push_str("=\"");
            write_escaped_string(out, label_value, true);
            out.push('"');
            separator = ',';
        }
        if let Some((label_name, label_value)) = additional_label {
            out.push(separator);
            out.push_str(label_name);
            out.push_str("=\"");
            write_float(out, label_value);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    write_float(out, value);
    out.push('\n');
}

// Backslash and newline, and the double quote in a label value.
fn write_escaped_string(out: &mut String, v: &str, include_double_quote: bool) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' if include_double_quote => out.push_str("\\\""),
            c => out.push(c),
        }
    }
}

/// A number as the Go client writes it: Go's `strconv.FormatFloat(f, 'g', -1, 64)`, with
/// `+Inf`, `-Inf` and `NaN`, and 0 for negative zero.
pub fn write_float(out: &mut String, f: f64) {
    if f == 1.0 {
        out.push('1');
    } else if f == 0.0 {
        out.push('0');
    } else if f == -1.0 {
        out.push_str("-1");
    } else if f.is_nan() {
        out.push_str("NaN");
    } else if f == f64::INFINITY {
        out.push_str("+Inf");
    } else if f == f64::NEG_INFINITY {
        out.push_str("-Inf");
    } else {
        format_float_g(out, f);
    }
}

// Go's %g with the shortest digits that read back as the same float: Rust's `{:e}` gives
// those digits (both take the shortest, and of several the nearest); Go writes them with an
// exponent when it is below -4 or at least 6 (`eprec = 6` for the shortest), as d.ddde±XX
// with at least two digits of exponent, and as a plain decimal otherwise.
fn format_float_g(out: &mut String, f: f64) {
    let e = format!("{:e}", f.abs());
    let (mantissa, exp) = e.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("the exponent of {:e} is a number");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let nd = digits.len() as i32;

    if f < 0.0 {
        out.push('-');
    }
    if !(-4..6).contains(&exp) {
        out.push_str(&digits[..1]);
        if nd > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let sign = if exp < 0 { '-' } else { '+' };
        let _ = write!(out, "e{sign}{:02}", exp.unsigned_abs());
        return;
    }
    // the position of the decimal point in the digits
    let dp = exp + 1;
    if dp <= 0 {
        out.push_str("0.");
        for _ in 0..-dp {
            out.push('0');
        }
        out.push_str(&digits);
    } else if dp >= nd {
        out.push_str(&digits);
        for _ in 0..dp - nd {
            out.push('0');
        }
    } else {
        out.push_str(&digits[..dp as usize]);
        out.push('.');
        out.push_str(&digits[dp as usize..]);
    }
}

/// The content type the Go client's handler sends with the text format, for the value of
/// the request's `Accept` header (`""` when it has none): `FMT_TEXT` and the escaping scheme
/// that `expfmt.Negotiate` settles on, which is the last one a known `escaping` parameter
/// names among the media ranges it goes through, in the order of their quality, until one
/// accepts a format it has (`underscores` if none names one).
///
/// The Go handler also has Prometheus' protobuf formats, and answers in one of them where
/// a media range that asks for it comes first; this serves the text format then too, with
/// the scheme as far as the negotiation got.
pub fn negotiate_text_format(accept: &str) -> String {
    let mut escaping_scheme = DEFAULT_ESCAPING_SCHEME;
    for ac in parse_accept(accept) {
        if let Some(escape_param) = ac.param("escaping")
            && matches!(
                escape_param,
                "allow-utf-8" | "underscores" | "dots" | "values"
            )
        {
            escaping_scheme = escape_param;
        }
        if ac.accepts_text() || ac.accepts_protobuf() {
            break;
        }
    }
    format!("{FMT_TEXT}; escaping={escaping_scheme}")
}

// A media range of an `Accept` header, as goautoneg (which expfmt uses) reads it.
struct Accept<'a> {
    r#type: &'a str,
    sub_type: &'a str,
    q: f64,
    params: Vec<(&'a str, &'a str)>,
}

impl<'a> Accept<'a> {
    // The value of the parameter: the last one of the name, as Go's map keeps it.
    fn param(&self, name: &str) -> Option<&'a str> {
        self.params
            .iter()
            .rev()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| *v)
    }

    // expfmt's matchFormat for FmtText: text/plain in version 0.0.4, which is the version
    // of a text/plain without one.
    fn accepts_text(&self) -> bool {
        if self.r#type != "*" && self.r#type != "text" {
            return false;
        }
        if self.sub_type != "*" && self.sub_type != "plain" {
            return false;
        }
        let mut version = self.param("version").unwrap_or("");
        if version.is_empty() && self.r#type == "text" && self.sub_type == "plain" {
            version = "0.0.4";
        }
        version == "0.0.4"
    }

    // expfmt's matchFormat for its three protobuf formats.
    fn accepts_protobuf(&self) -> bool {
        if self.r#type != "*" && self.r#type != "application" {
            return false;
        }
        if self.sub_type != "*" && self.sub_type != "vnd.google.protobuf" {
            return false;
        }
        self.param("proto") == Some("io.prometheus.client.MetricFamily")
            && matches!(
                self.param("encoding"),
                Some("delimited" | "text" | "compact-text")
            )
    }
}

// goautoneg's ParseAccept: the media ranges of the header, the ones it cannot read left out,
// sorted by quality. Its order is not consistent (a range that names a type comes before
// one that does not, whatever their quality), so its sort is followed as Go runs it: an
// insertion sort, which is what Go's sort.Sort does for up to twelve elements.
fn parse_accept(header: &str) -> Vec<Accept<'_>> {
    fn trim(s: &str) -> &str {
        s.trim_matches(' ')
    }
    // goautoneg's nextSplitElement: before the first separator, and after it
    fn next_split_element(s: &str, sep: char) -> (&str, &str) {
        s.split_once(sep).unwrap_or((s, ""))
    }

    let mut accept = Vec::new();
    for part in split_go(header, ',') {
        let part = trim(part);
        let (media_range, params) = next_split_element(part, ';');
        let (sp0, sp_remaining) = next_split_element(media_range, '/');
        let r#type = trim(sp0);
        let sub_type = if sp_remaining.is_empty() {
            if r#type != "*" {
                continue;
            }
            "*"
        } else {
            let (sp1, sp_remaining) = next_split_element(sp_remaining, '/');
            if !sp_remaining.is_empty() {
                continue;
            }
            trim(sp1)
        };
        let mut a = Accept {
            r#type,
            sub_type,
            q: 1.0,
            params: Vec::new(),
        };
        for param in split_go(params, ';') {
            let (token, sp_remaining) = next_split_element(param, '=');
            if sp_remaining.is_empty() {
                continue;
            }
            let (value, sp_remaining) = next_split_element(sp_remaining, '=');
            if !sp_remaining.is_empty() {
                continue;
            }
            let token = trim(token);
            if token == "q" {
                // strconv.ParseFloat(value, 32), its error ignored
                a.q = value.parse::<f32>().map_or(0.0, f64::from);
            } else {
                a.params.push((token, trim(value)));
            }
        }
        accept.push(a);
    }

    fn less(ai: &Accept, aj: &Accept) -> bool {
        if ai.q > aj.q {
            return true;
        }
        if ai.r#type != "*" && aj.r#type == "*" {
            return true;
        }
        if ai.sub_type != "*" && aj.sub_type == "*" {
            return true;
        }
        false
    }
    for i in 1..accept.len() {
        let mut j = i;
        while j > 0 && less(&accept[j], &accept[j - 1]) {
            accept.swap(j, j - 1);
            j -= 1;
        }
    }
    accept
}

// goautoneg's splitting: the pieces between separators, none after a separator at the end.
fn split_go(s: &str, sep: char) -> impl Iterator<Item = &str> {
    let s = s.strip_suffix(sep).unwrap_or(s);
    s.split(sep).filter(move |_| !s.is_empty())
}

#[cfg(test)]
#[path = "exposition_test.rs"]
mod exposition_test;

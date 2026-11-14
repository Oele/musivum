// New for musivum: the text format held against what the Go client LiveKit is built with
// writes for the same collectors and values (testdata/exposition_test.go wrote the files,
// and says how to write them again).

use super::super::collectors::{Counter, Gauge, Histogram, MetricVec};
use super::*;

const NODE: [(&str, &str); 2] = [("node_id", "ND_abc"), ("node_type", "SERVER")];

#[test]
fn test_text_equals_the_go_clients() {
    let zero = Counter::new();

    let counter = Counter::new();
    counter.add(1234567);

    let plain = Counter::new();
    plain.add(7);

    let counter_vec: MetricVec<Counter, 1> = MetricVec::new(["direction"], Counter::new);
    counter_vec.with_label_values(["outgoing"]).add(42);
    counter_vec.with_label_values(["incoming"]).inc();
    counter_vec.with_label_values(["a\"b\\c\nd"]).add(3);
    counter_vec.with_label_values(["é€ \t{x=\"1\"}"]).add(4);
    counter_vec.with_label_values([""]).add(5);

    let unused: MetricVec<Counter, 1> = MetricVec::new(["direction"], Counter::new);

    let gauge = Gauge::new();
    gauge.set(-0.5);

    let floats: MetricVec<Gauge, 1> = MetricVec::new(["case"], Gauge::new);
    for (name, value) in [
        ("zero", 0.0),
        ("neg_zero", -0.0),
        ("one", 1.0),
        ("minus_one", -1.0),
        ("half", 0.5),
        ("third", 1.0 / 3.0),
        ("frac", 1234.5678),
        ("e-4", 0.0001),
        ("e-5", 0.00001),
        ("neg_small", -2.5e-7),
        ("big", 123456.0),
        ("e6", 1e6),
        ("e6_plus", 1234567.0),
        ("neg_e6", -1e6),
        ("e21", 1e21),
        ("e100", 1e100),
        ("max", f64::MAX),
        ("min_positive", 5e-324),
        ("nan", f64::NAN),
        ("inf", f64::INFINITY),
        ("neg_inf", f64::NEG_INFINITY),
        ("two_pow_53_plus_one", ((1u64 << 53) + 1) as f64),
    ] {
        floats.with_label_values([name]).set(value);
    }

    let histogram = Histogram::new([0.005, 0.25, 1.0, 2.5, 1e6]);
    for v in [0.001, 0.3, 3.0, 2e6, 0.25] {
        histogram.observe(v);
    }

    // label names out of their order
    let histogram_vec: MetricVec<Histogram<2>, 2> =
        MetricVec::new(["type", "direction"], || Histogram::new([1.0, 10.0]));
    histogram_vec
        .with_label_values(["video", "incoming"])
        .observe(5.0);
    histogram_vec
        .with_label_values(["audio", "outgoing"])
        .observe(0.5);
    histogram_vec
        .with_label_values(["audio", "outgoing"])
        .observe(20.0);
    histogram_vec
        .with_label_values(["audio", "incoming"])
        .observe(1.0);

    // in the order they are registered in upstream's test
    let families = [
        MetricFamily::of("musivum_test_zero_total", "", &zero),
        MetricFamily::of("musivum_test_counter_total", "A counter.", &counter),
        MetricFamily::of_vec(
            "musivum_test_direction_total",
            "Help with a \\backslash,\na newline and \"quotes\".",
            &counter_vec,
        ),
        MetricFamily::of_vec(
            "musivum_test_unused_total",
            "A vector whose label values were never used.",
            &unused,
        ),
        MetricFamily::of("musivum_test_gauge", "A gauge.", &gauge),
        MetricFamily::of_vec("musivum_test_floats", "How numbers are written.", &floats),
        MetricFamily::of("musivum_test_histogram", "A histogram.", &histogram),
        MetricFamily::of_vec(
            "musivum_test_histogram_vec",
            "Histograms by two labels.",
            &histogram_vec,
        ),
    ]
    .map(|family| family.with_const_labels(&NODE));
    let mut families = families.to_vec();
    families.insert(
        2,
        MetricFamily::of(
            "musivum_test_plain_total",
            "Without constant labels.",
            &plain,
        ),
    );

    let text = encode_text(&families);
    let original = include_str!("testdata/go-text.txt");
    for (i, (port, original)) in text.lines().zip(original.lines()).enumerate() {
        assert_eq!(original, port, "line {}", i + 1);
    }
    assert_eq!(original, text);
}

#[test]
fn test_content_type_equals_the_go_clients() {
    let mut compared = 0;
    for line in include_str!("testdata/go-content-type.txt").lines() {
        let (accept, content_type) = line.split_once('\t').unwrap();
        assert_eq!(
            content_type,
            negotiate_text_format(accept),
            "Accept: {accept}"
        );
        compared += 1;
    }
    assert_eq!(17, compared);
}

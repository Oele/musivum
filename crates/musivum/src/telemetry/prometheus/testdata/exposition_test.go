// A test that is laid over pkg/telemetry/prometheus of LiveKit's server at the pin, to write
// down what the Go client LiveKit is built with (client_golang's promhttp and common's
// expfmt) answers for a set of collectors: go-text.txt is the body of the answer to a GET,
// go-content-type.txt the content type it gives for each Accept header below (a line each,
// the header and the content type separated by a tab). `exposition_test.rs` makes the same
// collectors with musivum's and holds the port against both. To write them again, from the
// repository's root:
//
//	d=$PWD/crates/musivum/src/telemetry/prometheus/testdata
//	r=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit
//	echo "{\"Replace\": {\"$r/pkg/telemetry/prometheus/musivum_exposition_test.go\": \"$d/exposition_test.go\"}}" > target/overlay-exposition.json
//	MUSIVUM_OUT=$d scripts/go-reference-test.sh -overlay $PWD/target/overlay-exposition.json -count=1 -run TestMusivumExposition ./pkg/telemetry/prometheus
package prometheus

import (
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

// The Accept headers whose content type is written down: none, Prometheus' own (version 3
// and version 2), and the ways a media range can name an escaping scheme or not match.
var musivumAccepts = []string{
	"",
	"application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.6,application/openmetrics-text;version=0.0.1;q=0.5,text/plain;version=1.0.0;escaping=allow-utf-8;q=0.4,text/plain;version=0.0.4;q=0.3,*/*;q=0.2",
	"application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1",
	"text/plain",
	"*/*",
	"*",
	"application/json",
	"text/plain;version=0.0.4;escaping=dots",
	"text/plain; version=0.0.4; escaping=values",
	"text/plain;escaping=bogus",
	"text/plain;version=0.0.4;escaping=values;q=0.1, text/plain;escaping=allow-utf-8;q=0.9",
	"text/*;escaping=allow-utf-8",
	"text/plain;version=1.0.0;escaping=dots",
	"*/*;escaping=values, text/plain;q=0.5",
	"text/html;escaping=dots;q=0.9, application/json;escaping=values",
	"text/plain;escaping=dots=x, text/plain/;escaping=values;q=0.5",
	"text/plain;q=x;escaping=dots, application/xml;escaping=allow-utf-8",
}

func TestMusivumExposition(t *testing.T) {
	out := os.Getenv("MUSIVUM_OUT")
	if out == "" {
		t.Skip("MUSIVUM_OUT is not set")
	}

	reg := prometheus.NewRegistry()
	node := prometheus.Labels{"node_id": "ND_abc", "node_type": "SERVER"}

	// registered out of the order of their names
	zero := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "musivum", Subsystem: "test", Name: "zero_total", ConstLabels: node,
	})
	reg.MustRegister(zero)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "musivum", Subsystem: "test", Name: "counter_total",
		Help: "A counter.", ConstLabels: node,
	})
	reg.MustRegister(counter)
	counter.Add(1234567)

	plain := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "musivum", Subsystem: "test", Name: "plain_total",
		Help: "Without constant labels.",
	})
	reg.MustRegister(plain)
	plain.Add(7)

	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "musivum", Subsystem: "test", Name: "direction_total",
		Help: "Help with a \\backslash,\na newline and \"quotes\".", ConstLabels: node,
	}, []string{"direction"})
	reg.MustRegister(counterVec)
	counterVec.WithLabelValues("outgoing").Add(42)
	counterVec.WithLabelValues("incoming").Inc()
	counterVec.WithLabelValues("a\"b\\c\nd").Add(3)
	counterVec.WithLabelValues("é€ \t{x=\"1\"}").Add(4)
	counterVec.WithLabelValues("").Add(5)

	unused := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "musivum", Subsystem: "test", Name: "unused_total",
		Help: "A vector whose label values were never used.", ConstLabels: node,
	}, []string{"direction"})
	reg.MustRegister(unused)

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "musivum", Subsystem: "test", Name: "gauge",
		Help: "A gauge.", ConstLabels: node,
	})
	reg.MustRegister(gauge)
	gauge.Set(-0.5)

	floats := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "musivum", Subsystem: "test", Name: "floats",
		Help: "How numbers are written.", ConstLabels: node,
	}, []string{"case"})
	reg.MustRegister(floats)
	for _, c := range []struct {
		name  string
		value float64
	}{
		{"zero", 0},
		{"neg_zero", math.Copysign(0, -1)},
		{"one", 1},
		{"minus_one", -1},
		{"half", 0.5},
		{"third", 1.0 / 3},
		{"frac", 1234.5678},
		{"e-4", 0.0001},
		{"e-5", 0.00001},
		{"neg_small", -2.5e-7},
		{"big", 123456},
		{"e6", 1e6},
		{"e6_plus", 1234567},
		{"neg_e6", -1e6},
		{"e21", 1e21},
		{"e100", 1e100},
		{"max", math.MaxFloat64},
		{"min_positive", 5e-324},
		{"nan", math.NaN()},
		{"inf", math.Inf(+1)},
		{"neg_inf", math.Inf(-1)},
		{"two_pow_53_plus_one", 9007199254740993},
	} {
		floats.WithLabelValues(c.name).Set(c.value)
	}

	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "musivum", Subsystem: "test", Name: "histogram",
		Help: "A histogram.", ConstLabels: node,
		Buckets: []float64{0.005, 0.25, 1, 2.5, 1e6},
	})
	reg.MustRegister(histogram)
	for _, v := range []float64{0.001, 0.3, 3, 2e6, 0.25} {
		histogram.Observe(v)
	}

	// label names out of their order
	histogramVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "musivum", Subsystem: "test", Name: "histogram_vec",
		Help: "Histograms by two labels.", ConstLabels: node,
		Buckets: []float64{1, 10},
	}, []string{"type", "direction"})
	reg.MustRegister(histogramVec)
	histogramVec.WithLabelValues("video", "incoming").Observe(5)
	histogramVec.WithLabelValues("audio", "outgoing").Observe(0.5)
	histogramVec.WithLabelValues("audio", "outgoing").Observe(20)
	histogramVec.WithLabelValues("audio", "incoming").Observe(1)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	require.Equal(t, 200, rec.Code)
	require.NoError(t, os.WriteFile(filepath.Join(out, "go-text.txt"), rec.Body.Bytes(), 0o644))

	var lines []string
	for _, accept := range musivumAccepts {
		req := httptest.NewRequest("GET", "/metrics", nil)
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		contentType := rec.Header().Get("Content-Type")
		// only answers in the text format: musivum has no other
		require.True(t, strings.HasPrefix(contentType, "text/plain; version=0.0.4"), accept)
		lines = append(lines, accept+"\t"+contentType)
	}
	require.NoError(t, os.WriteFile(filepath.Join(out, "go-content-type.txt"), []byte(strings.Join(lines, "\n")+"\n"), 0o644))
}

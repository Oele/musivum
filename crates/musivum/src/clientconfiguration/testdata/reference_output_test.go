// New for musivum, not a part of LiveKit: prints what LiveKit's package says about made-up
// clients and about pairs of versions. Its output is reference_output.txt, the expected
// values of ../reference_test.rs, which says the same about the same clients and versions
// with the port and says what is on a line.
//
// It runs inside LiveKit's package at the pin, without being put there. From the root of
// the repository, or of a git worktree of it (`reference` is in the main checkout), with the
// cap on memory and time that docs/porting-guide.md asks for:
//
//	d=$PWD/crates/musivum/src/clientconfiguration/testdata
//	p=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/reference/livekit/pkg/clientconfiguration
//	mkdir -p target && echo "{\"Replace\": {\"$p/reference_output_test.go\": \"$d/reference_output_test.go\"}}" > $PWD/target/clientconf_reference_overlay.json
//	(ulimit -d 8388608; timeout 600 scripts/go-reference-test.sh -count=1 \
//	    -overlay $PWD/target/clientconf_reference_overlay.json -run TestReferenceOutput -v ./pkg/clientconfiguration) | grep '^REF ' > $d/reference_output.txt
//
// With MUSIVUM_CLIENTCONF_ALL=1 in the environment it prints the same for several hundred
// more clients and pairs, which it makes from lists of values: that output is kept outside
// the repository, and ../reference_test.rs reads it when MUSIVUM_CLIENTCONF_REFERENCE names
// it.
package clientconfiguration

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/d5/tengo/v2"
	"golang.org/x/mod/semver"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils/must"
)

// Rules that are not LiveKit's, for what its own do not use: the other fields of a client,
// the other comparisons of versions, rules that set the same field, a rule that is not
// merged. ../reference_test.rs has the same table.
var refConfigurations = []ConfigurationItem{
	{
		Match: must.Get(NewScriptMatch(`c.protocol > 5 && c.browser != "firefox"`)),
		Configuration: &livekit.ClientConfiguration{
			ResumeConnection: livekit.ClientConfigSetting_ENABLED,
			ForceRelay:       livekit.ClientConfigSetting_DISABLED,
		},
		Merge: true,
	},
	{
		Match: must.Get(NewScriptMatch(`c.sdk == "android" || c.sdk == "react_native" || c.sdk == "99"`)),
		Configuration: &livekit.ClientConfiguration{
			Video:      &livekit.VideoConfiguration{HardwareEncoder: livekit.ClientConfigSetting_DISABLED},
			ForceRelay: livekit.ClientConfigSetting_ENABLED,
		},
		Merge: true,
	},
	{
		Match: must.Get(NewScriptMatch(`c.version >= "2.17.1" && c.version != "3"`)),
		Configuration: &livekit.ClientConfiguration{
			Video:            &livekit.VideoConfiguration{},
			Screen:           &livekit.VideoConfiguration{HardwareEncoder: livekit.ClientConfigSetting_ENABLED},
			ResumeConnection: livekit.ClientConfigSetting_UNSET,
			DisabledCodecs: &livekit.DisabledCodecs{
				Codecs: []*livekit.Codec{{Mime: "video/VP8", FmtpLine: "x=1"}},
			},
		},
		Merge: true,
	},
	{
		Match: must.Get(NewScriptMatch(`c.os_version == "14.5" || c.address == "192.0.2.7"`)),
		Configuration: &livekit.ClientConfiguration{
			DisabledCodecs: &livekit.DisabledCodecs{
				Codecs:  []*livekit.Codec{{Mime: "video/H265"}},
				Publish: []*livekit.Codec{{Mime: "video/VP8"}},
			},
		},
		Merge: true,
	},
	{
		Match: must.Get(NewScriptMatch(`c.protocol == 16 || c.version < "1.0.0-beta" || c.browser_version == "18.3"`)),
		Configuration: &livekit.ClientConfiguration{
			ResumeConnection: livekit.ClientConfigSetting_DISABLED,
		},
		Merge: false,
	},
}

func refClient(sdk int32, version string, protocol int32, os, osVersion, deviceModel, browser, browserVersion, address string) *livekit.ClientInfo {
	return &livekit.ClientInfo{
		Sdk:            livekit.ClientInfo_SDK(sdk),
		Version:        version,
		Protocol:       protocol,
		Os:             os,
		OsVersion:      osVersion,
		DeviceModel:    deviceModel,
		Browser:        browser,
		BrowserVersion: browserVersion,
		Address:        address,
	}
}

// The clients whose lines are kept in the repository.
func refPinnedClients() []*livekit.ClientInfo {
	xiaomi := "Xiaomi 2201117TI"
	clients := []*livekit.ClientInfo{
		// nothing sent
		{},
		// Safari, and versions on both sides of 18.3
		refClient(1, "2.17.1", 16, "macOS", "14.5", "", "Safari", "18.3", ""),
		refClient(1, "2.17.1", 17, "macOS", "15.3.1", "", "Safari", "18.3.1", "192.0.2.7"),
		refClient(1, "2.16.10", 17, "iOS", "18.4", "iPhone15,2", "Safari", "18.4", ""),
		refClient(1, "2.17.2", 15, "macOS", "", "", "safari", "18.10", ""),
		refClient(1, "", 0, "macOS", "", "", "safari", "18.2", ""),
		refClient(1, "3", 9, "macOS", "", "", "safari", "18", ""),
		refClient(1, "3.0.0", 9, "macOS", "", "", "safari", "19", ""),
		refClient(1, "1.0.0-alpha", 9, "iOS", "17.6.1", "", "safari", "17.6.1", ""),
		refClient(1, "1.0.0-rc.1", 9, "iOS", "", "", "safari", "", ""),
		{Browser: "safari", BrowserVersion: "18.3.0"},
		{Browser: "safari", BrowserVersion: "18.3.0-beta"},
		{Browser: "safari", BrowserVersion: "18.3.1-beta"},
		{Browser: "safari", BrowserVersion: "18.3.0+build"},
		// versions that are compared as strings
		{Browser: "safari", BrowserVersion: "18.03"},
		{Browser: "safari", BrowserVersion: "18.3.0.1"},
		{Browser: "safari", BrowserVersion: "9.1.2.3"},
		{Browser: "safari", BrowserVersion: "100.0.0.0"},
		{Browser: "safari", BrowserVersion: "100.0"},
		{Browser: "safari", BrowserVersion: "18.4 "},
		{Browser: "safari", BrowserVersion: " 18.4"},
		{Browser: "safari", BrowserVersion: "v18.4"},
		{Browser: "safari", BrowserVersion: "18.3-beta"},
		{Browser: "safari", BrowserVersion: "99999999999999999999.1"},
		// names of the browser
		{Browser: "SAFARI", BrowserVersion: "18.4", Os: "iOS"},
		{Browser: "SAFARİ", BrowserVersion: "18.4"},
		{Browser: "safari ", BrowserVersion: "18.4"},
		{Browser: "Mobile Safari", BrowserVersion: "18.4"},
		refClient(1, "2.17.1", 16, "linux", "", "", "Chrome", "120.0.6099.129", ""),
		// Firefox
		{Browser: "firefox", Os: "linux"},
		refClient(1, "2.17.1", 9, "macOS", "14.5", "", "Firefox", "155.0", ""),
		refClient(1, "2.17.1", 9, "android", "14", "Pixel 8", "Firefox Mobile", "156.0", ""),
		{Browser: "FIREFOX MOBILE", Os: "Android"},
		{Browser: "firefox", Os: "android"},
		{Browser: "firefox  mobile", Os: "android"},
		{Browser: "FİREFOX", Os: "android"},
		{Browser: "firefox", Os: "android "},
		{Browser: "firefox", Os: "ANDROİD"},
		// the device
		refClient(1, "2.17.1", 16, "Android", "13", xiaomi, "Chrome", "120.0.6099.129", ""),
		{Browser: "chrome", DeviceModel: "xiaomi 2201117ti"},
		refClient(3, "2.5.0", 12, "android", "13", "XIAOMI 2201117TI", "", "", "192.0.2.70"),
		{Os: "android", DeviceModel: "xiaomi 2201117ti "},
		{Os: "ios", DeviceModel: xiaomi},
		{Browser: "safari", BrowserVersion: "18.4", Os: "android", DeviceModel: xiaomi},
		{Browser: "firefox mobile", Os: "android", DeviceModel: xiaomi},
		// the other fields
		refClient(7, "2.17.1", 6, "ios", "14.5", "", "", "", ""),
		refClient(99, "2.17.0", 6, "", "", "", "", "", "192.0.2.7"),
		refClient(-1, "2.18.0-rc.1", 6, "", "14.5.1", "", "", "", "2001:db8::1"),
		refClient(15, "3", 2147483647, "Windows", "11", "", "", "", ""),
		refClient(16, "3.0.0+build", -1, "", "", "", "firefox", "", ""),
		refClient(2, "0.9", 12, "iOS", "17.0", "iPad13,4", "", "", ""),
	}
	return clients
}

// The pairs of versions whose lines are kept in the repository.
func refPinnedVersions() [][2]string {
	return [][2]string{
		{"18.3", "18.3"}, {"18.3.0", "18.3"}, {"18.3.1", "18.3"}, {"18.10", "18.3"}, {"18.2", "18.3"},
		{"18", "18.3"}, {"19", "18.3"}, {"", "18.3"}, {"18.03", "18.3"}, {"18.3.0.1", "18.3"},
		{"9.1.2.3", "18.3"}, {"100.0.0.0", "18.3"}, {"100.0", "18.3"}, {"18.4 ", "18.3"},
		{"v18.4", "18.3"}, {"18.3-beta", "18.3"}, {"18.3.0-beta", "18.3"}, {"18.3.0+build", "18.3"},
		{"99999999999999999999.1", "18.3"},
		{"2.17.1", "2.16.10"}, {"2.17.1", "2.17.1"}, {"2.9.0", "2.16.10"}, {"2.17", "2.17.0"},
		{"2.17.1-rc.1", "2.17.1"},
		{"1.0.0-alpha", "1.0.0-alpha.1"}, {"1.0.0-alpha.1", "1.0.0-alpha.beta"},
		{"1.0.0-alpha.beta", "1.0.0-beta"}, {"1.0.0-beta.2", "1.0.0-beta.11"},
		{"1.0.0-beta.11", "1.0.0-rc.1"}, {"1.0.0-rc.1", "1.0.0"}, {"1.0.0-2", "1.0.0-10"},
		{"1.0.0-10", "1.0.0-a"}, {"1.0.0-a", "1.0.0-A"}, {"1.0.0-01", "1.0.0-1"},
		{"1.0.0+a", "1.0.0+b"}, {"1.0.0-a+b", "1.0.0-a"}, {"1.0.0-", "1.0.0"}, {"1.0.0--", "1.0.0-a"},
		{"", ""}, {"abc", "abd"}, {"01", "1"}, {"1.2.3", "1.2.3.4"},
	}
}

// A sequence of numbers that is the same each time, to pick values with.
type refPicker uint32

func (p *refPicker) pick(values []string) string {
	*p = *p*1664525 + 1013904223
	return values[int(*p>>16)%len(values)]
}

func (p *refPicker) pickInt(values []int32) int32 {
	*p = *p*1664525 + 1013904223
	return values[int(*p>>16)%len(values)]
}

var (
	refSdks = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 99, -1}

	refProtocols = []int32{0, 2, 5, 6, 9, 16, 17, -1, 2147483647}

	refVersions = []string{
		"", "2.17.1", "2.17.0", "2.16.10", "2.18", "1.0.0-alpha", "1.0.0-beta", "1.0.0", "3", "3.0.0+build",
		"2.17.1.1", "v2.17.1", "0.9",
	}

	refOsVersions = []string{"", "14.5", "14.5.1", "Sonoma", "10"}

	refAddresses = []string{"", "192.0.2.7", "2001:db8::1", "192.0.2.70"}

	refSafaris = []string{"safari", "Safari", "SAFARI", "SAFARİ", "safari ", "mobile safari", "chrome"}

	refBrowsers = []string{
		"", "safari", "Safari", "SAFARI", "SAFARİ", "safarı", "ſafari", "safari ", " safari", "safari mobile",
		"mobile safari", "firefox", "Firefox", "FIREFOX", "FİREFOX", "firefox mobile", "Firefox Mobile",
		"FIREFOX MOBİLE", "firefox  mobile", "firefoxmobile", "firefox-mobile", "firefox mobile ", "chrome",
		"Chrome", "edge", "ΟΔΥΣΣΕΥΣ", "ǅ", "ẞ", "KELVİN",
	}

	refBrowserVersions = []string{
		"", "18.3", "18.3.0", "18.3.1", "18.4", "18.2", "18.2.9", "18.10", "18.30", "18", "19", "17.6", "17.6.1",
		"9.1", "100.0", "18.3.0.1", "18.2.9.9", "9.1.2.3", "100.0.0.0", "18.03", "018.4", "18.3.00", "18.3-beta",
		"18.3.0-beta", "18.3.1-beta", "18.3.0+build", "18.4.0-0", "18.4.0-01", "v18.4", "18.4 ", " 18.4", "18,4",
		"18.x", "abc", "18.3.", ".18.4", "1e3", "１８.４", "99999999999999999999.1", "18.99999999999999999999",
		"18.3.0-", "18.3.0+", "18.4.0-a..b", "18.4.0+a+b",
	}

	refOses = []string{
		"", "android", "Android", "ANDROID", "ANDROİD", "android ", "androıd", "ios", "iOS", "macOS", "linux",
		"windows",
	}

	refDeviceModels = []string{
		"", "xiaomi 2201117ti", "Xiaomi 2201117TI", "XIAOMI 2201117TI", "XİAOMİ 2201117Tİ", "xiaomi 2201117ti ",
		"xiaomi  2201117ti", "2201117ti", "Pixel 8", "iPhone15,2",
	}

	refVersionsLeft = []string{
		"1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11",
		"1.0.0-rc.1", "1.0.0", "1.0.0-1", "1.0.0-2", "1.0.0-10", "1.0.0-a-b", "1.0.0--", "1.0.0-", "1.0.0+",
		"1.0.0+a.b", "1.0.0+a..b", "1.0.0-a..b", "1.0.0-a+b+c", "1.0.0-a+b", "1.0.0-01", "1.0.0-0", "1.0.0-A",
		"1.0.0-a", "1.0", "1", "01", "1.01", "1.0.01", "0", "0.0.0", "1.0.1", "1.1", "2", "10", "1.0.0.0",
		"2.16.10", "2.17.1", "2.17", "2.9.0", "11.3", "13.2", "abc", "abd", "v1.0.0", "1.0.0 ", "-1", "+1",
		"1.-1", "1..1", "é", "1.0.0-é",
	}

	refVersionsRight = []string{
		"18.3", "2.16.10", "2.17.1", "11.3", "13.2", "", "1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-beta.2",
		"1.0.0-rc.1", "1.0.0", "1.0.0-a", "1.0.0-10", "1", "1.0", "abc", "18.3.0+build", "1.0.0-", "01", "1.0.0 ",
	}
)

// The clients that are made from the lists of values.
func refGeneratedClients() []*livekit.ClientInfo {
	var clients []*livekit.ClientInfo
	p := refPicker(1)
	other := func(os, deviceModel, browser, browserVersion string) *livekit.ClientInfo {
		return refClient(p.pickInt(refSdks), p.pick(refVersions), p.pickInt(refProtocols), os,
			p.pick(refOsVersions), deviceModel, browser, browserVersion, p.pick(refAddresses))
	}
	// every SDK, with nothing else and with a version
	for _, sdk := range refSdks {
		clients = append(clients, refClient(sdk, "", 0, "", "", "", "", "", ""))
		clients = append(clients, refClient(sdk, "2.17.1", 9, "", "", "", "", "", ""))
	}
	// Safari and what is nearly Safari, with every version
	for _, browser := range refSafaris {
		for _, browserVersion := range refBrowserVersions {
			clients = append(clients, other(p.pick(refOses), p.pick(refDeviceModels), browser, browserVersion))
		}
	}
	// every browser on some systems, with nothing else and with the rest
	for _, browser := range refBrowsers {
		for _, os := range []string{"", "android", "Android", "linux"} {
			clients = append(clients, &livekit.ClientInfo{Browser: browser, Os: os})
			clients = append(clients, other(os, p.pick(refDeviceModels), browser, p.pick(refBrowserVersions)))
		}
	}
	// every device on every system, without a browser and with Firefox
	for _, deviceModel := range refDeviceModels {
		for _, os := range refOses {
			for _, browser := range []string{"", "firefox", "Firefox Mobile"} {
				clients = append(clients, other(os, deviceModel, browser, p.pick(refBrowserVersions)))
			}
		}
	}
	return clients
}

// The pairs of versions that are made from the lists of values.
func refGeneratedVersions() [][2]string {
	var pairs [][2]string
	left := append(append([]string{}, refBrowserVersions...), refVersionsLeft...)
	for _, l := range left {
		for _, r := range refVersionsRight {
			pairs = append(pairs, [2]string{l, r})
		}
	}
	return pairs
}

// A value in a line: lines are split at tabs.
func refValue(s string) string {
	if strings.ContainsAny(s, "\t\r\n") {
		panic("a value with a tab or a line end: " + strconv.Quote(s))
	}
	return s
}

// What the script sees as c.<name>.
func refField(info *livekit.ClientInfo, name string) string {
	o, err := (&clientObject{info: info}).IndexGet(&tengo.String{Value: name})
	if err != nil {
		panic(err)
	}
	switch o := o.(type) {
	case *tengo.String:
		return refValue(o.Value)
	case *tengo.Int:
		return strconv.FormatInt(o.Value, 10)
	case *ruleSdkVersion:
		return refValue(o.sdkVersion)
	}
	panic("c." + name + " is a " + o.TypeName())
}

func refCodecs(codecs []*livekit.Codec) string {
	var s []string
	for _, c := range codecs {
		if c.FmtpLine != "" {
			s = append(s, c.Mime+";"+c.FmtpLine)
		} else {
			s = append(s, c.Mime)
		}
	}
	return "[" + strings.Join(s, ",") + "]"
}

func refVideo(v *livekit.VideoConfiguration) string {
	if v == nil {
		return "nil"
	}
	return strconv.Itoa(int(v.HardwareEncoder))
}

func refConfiguration(conf *livekit.ClientConfiguration) string {
	if conf == nil {
		return "nil"
	}
	disabled := "nil"
	if conf.DisabledCodecs != nil {
		disabled = "codecs=" + refCodecs(conf.DisabledCodecs.Codecs) + " publish=" + refCodecs(conf.DisabledCodecs.Publish)
	}
	return fmt.Sprintf("video=%s screen=%s resume=%d disabled={%s} relay=%d",
		refVideo(conf.Video), refVideo(conf.Screen), conf.ResumeConnection, disabled, conf.ForceRelay)
}

func refBool(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func TestReferenceOutput(t *testing.T) {
	clients := refPinnedClients()
	versions := refPinnedVersions()
	if os.Getenv("MUSIVUM_CLIENTCONF_ALL") != "" {
		clients = append(clients, refGeneratedClients()...)
		versions = append(versions, refGeneratedVersions()...)
	}

	cm := NewStaticClientConfigurationManager(StaticConfigurations)
	refCm := NewStaticClientConfigurationManager(refConfigurations)
	for _, info := range clients {
		line := []string{
			"REF client",
			strconv.Itoa(int(info.Sdk)), refValue(info.Version), strconv.Itoa(int(info.Protocol)), refValue(info.Os),
			refValue(info.OsVersion), refValue(info.DeviceModel), refValue(info.Browser),
			refValue(info.BrowserVersion), refValue(info.Address),
			"=>",
		}
		for _, name := range []string{
			"sdk", "version", "protocol", "os", "os_version", "device_model", "browser", "browser_version", "address",
		} {
			line = append(line, refField(info, name))
		}
		line = append(line, "=>", refConfiguration(cm.GetConfiguration(info)))
		line = append(line, "=>", refConfiguration(refCm.GetConfiguration(info)))
		fmt.Println(strings.Join(line, "\t"))
	}

	// what the scripts `c.version < "<right>"` and so on give for a client with the left
	// version, the browser's version taking turns with the SDK's, and what the package
	// that compares semantic versions says of the two
	ops := []string{"<", "<=", ">", ">=", "==", "!="}
	compiled := map[string][]*ScriptMatch{}
	for n, pair := range versions {
		left, right := pair[0], pair[1]
		field := []string{"version", "browser_version"}[n%2]
		key := field + "\t" + right
		if compiled[key] == nil {
			for _, op := range ops {
				expr := fmt.Sprintf("c.%s %s %s", field, op, strconv.Quote(right))
				compiled[key] = append(compiled[key], must.Get(NewScriptMatch(expr)))
			}
		}
		info := &livekit.ClientInfo{Version: left}
		if field == "browser_version" {
			info = &livekit.ClientInfo{BrowserVersion: left}
		}
		results := ""
		for _, match := range compiled[key] {
			matched, err := match.Match(info)
			if err != nil {
				results += "E"
			} else {
				results += refBool(matched)
			}
		}
		fmt.Println(strings.Join([]string{
			"REF version", field, refValue(left), refValue(right), "=>", results,
			refBool(semver.IsValid("v"+left)) + refBool(semver.IsValid("v"+right)),
			strconv.Itoa(semver.Compare("v"+left, "v"+right)),
		}, "\t"))
	}
}

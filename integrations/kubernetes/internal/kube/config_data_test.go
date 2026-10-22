package kube

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// configDataPEM holds two deterministic self-signed Ed25519 fixture CAs.
const configDataPEM = `-----BEGIN CERTIFICATE-----
MIIBMDCB46ADAgECAgEBMAUGAytlcDAgMR4wHAYDVQQDExVza3lnbyBkYXRhIGZp
eHR1cmUgQ0EwHhcNMjYwMTAxMDAwMDAwWhcNMzYwMTAxMDAwMDAwWjAgMR4wHAYD
VQQDExVza3lnbyBkYXRhIGZpeHR1cmUgQ0EwKjAFBgMrZXADIQB5tVYuj+ZU+UB4
sRLoqYunkB+FOuaVvtfg45ELrQSWZKNCMEAwDgYDVR0PAQH/BAQDAgIEMA8GA1Ud
EwEB/wQFMAMBAf8wHQYDVR0OBBYEFE4c8bsVIM0NmpnuH0rnUhZH3WpTMAUGAytl
cANBADPx7UmzZJAGEz6/Q9eTZTJyvSdMlDQTX+9alOm/VBXFxrNCIQaisjutys3P
wtVscj0xRSl9SQeWu2HNVN25cw0=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIBNDCB56ADAgECAgEBMAUGAytlcDAiMSAwHgYDVQQDExdza3lnbyBkYXRhIGZp
eHR1cmUgQ0EgMjAeFw0yNjAxMDEwMDAwMDBaFw0zNjAxMDEwMDAwMDBaMCIxIDAe
BgNVBAMTF3NreWdvIGRhdGEgZml4dHVyZSBDQSAyMCowBQYDK2VwAyEAebVWLo/m
VPlAeLES6KmLp5AfhTrmlb7X4OORC60ElmSjQjBAMA4GA1UdDwEB/wQEAwICBDAP
BgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBROHPG7FSDNDZqZ7h9K51IWR91qUzAF
BgMrZXADQQDSoB+Jai56xa66ReDGXXF6X3IN1zqIQfloIMc6oIckVwqMbPPckFWA
K+0iyBdyZsadLoqJ7lLOWFbdonNQqKwO
-----END CERTIFICATE-----
`

var (
	configDataDigest = strings.Repeat("ab", 32)
	configDataHelper = "registry.example.com/skygo/helper@sha256:" + configDataDigest
	configDataImageA = "registry.example.com/team/train@sha256:" + strings.Repeat("0a", 32)
	configDataImageB = "localhost:5000/eval@sha256:" + strings.Repeat("1b", 32)
)

func configDataFixture() *DataConfig {
	return &DataConfig{
		HelperImage:  configDataHelper,
		HelperSHA256: strings.Repeat("cd", 32),
		Origin:       "https://data.example.com:8443",
		CAPEM:        configDataPEM,
		TaskImages:   []string{configDataImageA, configDataImageB},
		References: []DataReference{
			{Name: "models", PVC: "model-cache", Destinations: []string{"/mnt/skygo/models", "/mnt/skygo/cache"}, AllowWrite: true},
			{Name: "creds", Secret: "registry-creds", Keys: []string{"token", "ca.crt"}, Destinations: []string{"/mnt/skygo/creds"}},
		},
	}
}

// configDataDocument returns a target document with the data block indented
// beneath one target.
func configDataDocument(data string) string {
	return "version: 1\ntargets:\n- name: worker\n  namespace: jobs\n  in_cluster: true\n  data:\n" +
		configDataIndent(data, "    ")
}

func configDataIndent(text, prefix string) string {
	var b strings.Builder
	for _, line := range strings.SplitAfter(text, "\n") {
		if line != "" {
			b.WriteString(prefix + line)
		}
	}
	return b.String()
}

func configDataBlock() string { return configDataBlockCA(configDataPEM) }

func configDataBlockCA(ca string) string {
	return "helper_image: " + configDataHelper + "\n" +
		"helper_sha256: " + strings.Repeat("cd", 32) + "\n" +
		"origin: https://data.example.com:443\n" +
		"ca_pem: |\n" + configDataIndent(ca, "  ") +
		"task_images:\n- " + configDataImageB + "\n- " + configDataImageA + "\n" +
		"references:\n" +
		"- name: models\n  pvc: model-cache\n  allow_write: true\n  destinations: [/mnt/skygo/models, /mnt/skygo/cache]\n" +
		"- name: creds\n  secret: registry-creds\n  keys: [token, ca.crt]\n  destinations: [/mnt/skygo/creds]\n"
}

func TestDataConfigRead(t *testing.T) {
	c, err := ReadConfig(configFile(t, configDataDocument(configDataBlock())), false)
	if err != nil {
		t.Fatal(err)
	}
	d := c.Targets()[0].Data
	if d == nil || d.CAPEM != configDataPEM || d.Origin != "https://data.example.com:443" || len(d.References) != 2 || !d.References[0].AllowWrite {
		t.Fatalf("data block differs: %+v", d)
	}
	p, err := newDataProfile(d)
	if err != nil {
		t.Fatal(err)
	}
	if p.origin != "https://data.example.com" || p.host != "data.example.com" || string(p.caPEM) != configDataPEM {
		t.Fatal("origin or certificate authority differs")
	}
	if !reflect.DeepEqual(p.taskImages, []string{configDataImageB, configDataImageA}) {
		t.Fatalf("task images not sorted: %q", p.taskImages)
	}
	want := map[string]dataReference{
		"models": {pvc: "model-cache", keys: []string{}, destinations: []string{"/mnt/skygo/cache", "/mnt/skygo/models"}, allowWrite: true},
		"creds":  {secret: "registry-creds", keys: []string{"ca.crt", "token"}, destinations: []string{"/mnt/skygo/creds"}},
	}
	if !reflect.DeepEqual(p.references, want) {
		t.Fatalf("references = %+v", p.references)
	}

	legacy, err := ReadConfig(configFile(t, "version: 1\ntargets:\n- name: worker\n  namespace: jobs\n  in_cluster: true\n"), false)
	if err != nil || legacy.Targets()[0].Data != nil {
		t.Fatal("absent data block is not nil")
	}
}

func TestDataProfileNil(t *testing.T) {
	p, err := newDataProfile(nil)
	if p != nil || err != nil {
		t.Fatal("nil data configuration is not a legacy target")
	}
}

func TestDataProfileFrozen(t *testing.T) {
	c := configDataFixture()
	p, err := newDataProfile(c)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := newDataProfile(configDataFixture())
	c.TaskImages[0] = "changed"
	c.References[0].Destinations[0] = "changed"
	c.References[1].Keys[0] = "changed"
	c.References[1].Name = "changed"
	if !reflect.DeepEqual(p, want) {
		t.Fatal("profile shares memory with its configuration")
	}
	if p.caSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(configDataPEM))) {
		t.Fatal("certificate digest differs")
	}
}

func TestDataImage(t *testing.T) {
	hex := strings.Repeat("0f", 32)
	for _, s := range []string{
		"registry.example.com/helper@sha256:" + hex,
		"registry.example.com/team/sub/helper@sha256:" + hex,
		"registry.example.com:5000/helper@sha256:" + hex,
		"localhost/helper@sha256:" + hex,
		"localhost:5000/helper@sha256:" + hex,
		"registry:5000/helper@sha256:" + hex,
		"reg-1.example/a_b__c-d.e--f@sha256:" + hex,
		"r.example/" + strings.Repeat("a", 255-len("r.example/")) + "@sha256:" + hex,
	} {
		if !configImage(s) {
			t.Errorf("refused %q", s)
		}
	}
	for _, s := range []string{
		"",
		"helper@sha256:" + hex,
		"library/helper@sha256:" + hex,
		"registry.example.com@sha256:" + hex,
		"registry.example.com/helper:v1@sha256:" + hex,
		"registry.example.com/helper:v1",
		"registry.example.com/helper",
		"Registry.example.com/helper@sha256:" + hex,
		"registry.example.com/Helper@sha256:" + hex,
		"registry.example.com/helper@sha256:" + strings.ToUpper(hex),
		"registry.example.com/helper@sha256:" + hex[:63],
		"registry.example.com/helper@sha256:" + hex + "0",
		"registry.example.com/helper@sha512:" + hex,
		"registry.example.com/helper@sha256:" + hex + "@sha256:" + hex,
		"registry.example.com:0/helper@sha256:" + hex,
		"registry.example.com:05000/helper@sha256:" + hex,
		"registry.example.com:65536/helper@sha256:" + hex,
		"registry.example.com:/helper@sha256:" + hex,
		"registry.example.com./helper@sha256:" + hex,
		"-registry.example.com/helper@sha256:" + hex,
		"[::1]:5000/helper@sha256:" + hex,
		"registry.example.com//helper@sha256:" + hex,
		"registry.example.com/helper/@sha256:" + hex,
		"registry.example.com/-helper@sha256:" + hex,
		"registry.example.com/helper_@sha256:" + hex,
		"registry.example.com/a___b@sha256:" + hex,
		"registry.example.com/a..b@sha256:" + hex,
		"registry.example.com/a b@sha256:" + hex,
		"r.example/" + strings.Repeat("a", 256-len("r.example/")) + "@sha256:" + hex,
	} {
		if configImage(s) {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestDataOrigin(t *testing.T) {
	for _, test := range []struct{ input, origin, host string }{
		{"https://data.example.com", "https://data.example.com", "data.example.com"},
		{"https://data.example.com:443", "https://data.example.com", "data.example.com"},
		{"https://data.example.com:8443", "https://data.example.com:8443", "data.example.com"},
		{"https://data.example.com:1", "https://data.example.com:1", "data.example.com"},
		{"https://data.example.com:65535", "https://data.example.com:65535", "data.example.com"},
		{"https://localhost:9000", "https://localhost:9000", "localhost"},
		{"https://a-1.b2.example", "https://a-1.b2.example", "a-1.b2.example"},
	} {
		origin, host, ok := configOrigin(test.input)
		if !ok || origin != test.origin || host != test.host {
			t.Errorf("configOrigin(%q) = %q, %q, %v", test.input, origin, host, ok)
		}
	}
	for _, s := range []string{
		"", "https://", "data.example.com", "http://data.example.com", "HTTPS://data.example.com",
		"https://Data.example.com", "https://data.example.com/", "https://data.example.com/path",
		"https://data.example.com?", "https://data.example.com?a=b", "https://data.example.com#",
		"https://user@data.example.com", "https://user:pass@data.example.com",
		"https://data.example.com.", "https://data.example.com.:443", "https://data.example.com:",
		"https://data.example.com:0", "https://data.example.com:65536", "https://data.example.com:0443",
		"https://data.example.com:+443", "https://data.example.com:44a", "https://data.example.com:443:1",
		"https://127.0.0.1", "https://127.1", "https://0x7f.1", "https://10.0.0.1:8443",
		"https://[::1]", "https://[::1]:443", "https://[fe80::1%25en0]", "https://data%2eexample.com",
		"https://data .example.com", " https://data.example.com", "https://data.example.com\n",
		"https://data\\example.com", "https://data.example.com\\", "https://dätä.example.com",
		"https://-data.example.com", "https://data..example.com", "https://data_x.example.com",
		"https://" + strings.Repeat("a", 64) + ".example.com", "https:///data.example.com",
	} {
		if _, _, ok := configOrigin(s); ok {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestDataCertificates(t *testing.T) {
	first := configDataPEM[:strings.Index(configDataPEM, "-----END CERTIFICATE-----")+len("-----END CERTIFICATE-----\n")]
	for _, test := range []struct {
		name, pem string
		ok        bool
	}{
		{"two", configDataPEM, true},
		{"one", first, true},
		{"surrounding whitespace", "\n\n " + configDataPEM + "\n\t\n", true},
		{"no trailing newline", strings.TrimSuffix(first, "\n"), true},
		{"large bundle", strings.Repeat(configDataPEM, configMaxCA/len(configDataPEM)), true},
		{"empty", "", false},
		{"whitespace", " \n", false},
		{"garbage", "not a certificate", false},
		{"leading text", "comment\n" + first, false},
		{"trailing text", first + "comment\n", false},
		{"trailing partial", first + "-----BEGIN CERTIFICATE-----\n", false},
		{"private key", strings.ReplaceAll(first, "CERTIFICATE", "PRIVATE KEY"), false},
		{"trusted certificate", strings.ReplaceAll(first, "CERTIFICATE", "TRUSTED CERTIFICATE"), false},
		{"header", strings.Replace(first, "-----\n", "-----\nProc-Type: 4,ENCRYPTED\n\n", 1), false},
		{"bad der", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n", false},
		{"oversize", strings.Repeat(configDataPEM, configMaxCA/len(configDataPEM)+1), false},
	} {
		if got := configCertificates([]byte(test.pem)); got != test.ok {
			t.Errorf("%s: accepted=%v, want %v", test.name, got, test.ok)
		}
	}
}

func TestDataProfileRules(t *testing.T) {
	many := func(n int, f func(int) string) []string {
		var s []string
		for i := range n {
			s = append(s, f(i))
		}
		return s
	}
	refs := func(n int) []DataReference {
		var r []DataReference
		for i := range n {
			r = append(r, DataReference{Name: fmt.Sprintf("ref-%d", i), PVC: "claim", Destinations: []string{"/mnt/skygo/a"}})
		}
		return r
	}
	image := func(i int) string {
		return fmt.Sprintf("registry.example.com/image-%d@sha256:%s", i, configDataDigest)
	}
	dest := func(i int) string { return fmt.Sprintf("/mnt/skygo/d%d", i) }
	key := func(i int) string { return fmt.Sprintf("k%d", i) }
	for _, test := range []struct {
		name   string
		change func(*DataConfig)
		ok     bool
	}{
		{"fixture", func(c *DataConfig) {}, true},
		{"helper image", func(c *DataConfig) { c.HelperImage = "helper:latest" }, false},
		{"helper tag", func(c *DataConfig) { c.HelperImage = "registry.example.com/helper:v1@sha256:" + configDataDigest }, false},
		{"helper digest", func(c *DataConfig) { c.HelperSHA256 = strings.ToUpper(c.HelperSHA256) }, false},
		{"helper digest short", func(c *DataConfig) { c.HelperSHA256 = c.HelperSHA256[1:] }, false},
		{"helper digest prefixed", func(c *DataConfig) { c.HelperSHA256 = "sha256:" + c.HelperSHA256[7:] }, false},
		{"no task images", func(c *DataConfig) { c.TaskImages = nil }, false},
		{"32 task images", func(c *DataConfig) { c.TaskImages = many(32, image) }, true},
		{"33 task images", func(c *DataConfig) { c.TaskImages = many(33, image) }, false},
		{"duplicate task image", func(c *DataConfig) { c.TaskImages = []string{configDataImageA, configDataImageB, configDataImageA} }, false},
		{"invalid task image", func(c *DataConfig) { c.TaskImages = []string{configDataImageA, "ubuntu"} }, false},
		{"origin", func(c *DataConfig) { c.Origin = "http://data.example.com" }, false},
		{"origin path", func(c *DataConfig) { c.Origin = "https://data.example.com/" }, false},
		{"ca", func(c *DataConfig) { c.CAPEM = "" }, false},
		{"no references", func(c *DataConfig) { c.References = nil }, true},
		{"16 references", func(c *DataConfig) { c.References = refs(16) }, true},
		{"17 references", func(c *DataConfig) { c.References = refs(17) }, false},
		{"duplicate reference", func(c *DataConfig) { c.References[1].Name = "models" }, false},
		{"reference name", func(c *DataConfig) { c.References[0].Name = "Models" }, false},
		{"reference name dot", func(c *DataConfig) { c.References[0].Name = "a.b" }, false},
		{"reference name 63", func(c *DataConfig) { c.References[0].Name = strings.Repeat("a", 63) }, true},
		{"reference name 64", func(c *DataConfig) { c.References[0].Name = strings.Repeat("a", 64) }, false},
		{"pvc and secret", func(c *DataConfig) { c.References[0].Secret = "other" }, false},
		{"neither pvc nor secret", func(c *DataConfig) { c.References[0].PVC = "" }, false},
		{"pvc name", func(c *DataConfig) { c.References[0].PVC = "Claim" }, false},
		{"pvc subdomain", func(c *DataConfig) { c.References[0].PVC = "claim.v1" }, true},
		{"pvc keys", func(c *DataConfig) { c.References[0].Keys = []string{"k"} }, false},
		{"pvc read only", func(c *DataConfig) { c.References[0].AllowWrite = false }, true},
		{"secret name", func(c *DataConfig) { c.References[1].Secret = "bad/name" }, false},
		{"secret no keys", func(c *DataConfig) { c.References[1].Keys = nil }, false},
		{"secret 32 keys", func(c *DataConfig) { c.References[1].Keys = many(32, key) }, true},
		{"secret 33 keys", func(c *DataConfig) { c.References[1].Keys = many(33, key) }, false},
		{"secret duplicate key", func(c *DataConfig) { c.References[1].Keys = []string{"a", "b", "a"} }, false},
		{"secret key slash", func(c *DataConfig) { c.References[1].Keys = []string{"a/b"} }, false},
		{"secret key dotdot", func(c *DataConfig) { c.References[1].Keys = []string{".."} }, false},
		{"secret key dot", func(c *DataConfig) { c.References[1].Keys = []string{"."} }, false},
		{"secret key space", func(c *DataConfig) { c.References[1].Keys = []string{"a b"} }, false},
		{"secret key empty", func(c *DataConfig) { c.References[1].Keys = []string{""} }, false},
		{"secret allow write", func(c *DataConfig) { c.References[1].AllowWrite = true }, false},
		{"no destinations", func(c *DataConfig) { c.References[0].Destinations = nil }, false},
		{"16 destinations", func(c *DataConfig) { c.References[0].Destinations = many(16, dest) }, true},
		{"17 destinations", func(c *DataConfig) { c.References[0].Destinations = many(17, dest) }, false},
		{"duplicate destination", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a", "/mnt/skygo/a"} }, false},
		{"nested destination", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a/b", "/mnt/skygo/a"} }, false},
		{"sibling prefix destination", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a", "/mnt/skygo/ab"} }, true},
		{"cross-reference destination", func(c *DataConfig) { c.References[1].Destinations = []string{"/mnt/skygo/models"} }, true},
		{"outside root", func(c *DataConfig) { c.References[0].Destinations = []string{"/data/x"} }, false},
		{"root itself", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo"} }, false},
		{"root slash", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/"} }, false},
		{"relative", func(c *DataConfig) { c.References[0].Destinations = []string{"mnt/skygo/a"} }, false},
		{"dot", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/./a"} }, false},
		{"dotdot", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/../run/skygo"} }, false},
		{"trailing slash", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a/"} }, false},
		{"empty component", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo//a"} }, false},
		{"colon", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a:b"} }, false},
		{"glob", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a*"} }, false},
		{"bracket", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/[a]"} }, false},
		{"backslash", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a\\b"} }, false},
		{"NUL", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a\x00"} }, false},
		{"control", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/a\nb"} }, false},
		{"invalid UTF-8", func(c *DataConfig) { c.References[0].Destinations = []string{"/mnt/skygo/\xff"} }, false},
		{"240 bytes", func(c *DataConfig) {
			c.References[0].Destinations = []string{"/mnt/skygo/" + strings.Repeat("a", 240-len("/mnt/skygo/"))}
		}, true},
		{"241 bytes", func(c *DataConfig) {
			c.References[0].Destinations = []string{"/mnt/skygo/" + strings.Repeat("a", 241-len("/mnt/skygo/"))}
		}, false},
		{"16 components", func(c *DataConfig) {
			c.References[0].Destinations = []string{"/mnt/skygo" + strings.Repeat("/a", 14)}
		}, true},
		{"17 components", func(c *DataConfig) {
			c.References[0].Destinations = []string{"/mnt/skygo" + strings.Repeat("/a", 15)}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := configDataFixture()
			test.change(c)
			target := TargetConfig{Name: "worker", Namespace: "jobs", InCluster: true, Data: c}
			err := validateTarget(target)
			if (err == nil) != test.ok {
				t.Fatalf("accepted=%v, want %v; %v", err == nil, test.ok, err)
			}
			if err != nil {
				for _, value := range []string{"registry.example.com", "data.example.com", "model-cache", "/mnt/skygo", "CERTIFICATE"} {
					if strings.Contains(err.Error(), value) {
						t.Fatalf("diagnostic exposed input: %v", err)
					}
				}
			}
		})
	}
}

func TestDataConfigStrictYAML(t *testing.T) {
	block := configDataBlock()
	for _, test := range []struct {
		name, text string
		ok         bool
	}{
		{"fixture", configDataDocument(block), true},
		{"allow_write false", configDataDocument(strings.Replace(block, "allow_write: true", "allow_write: false", 1)), true},
		{"allow_write omitted", configDataDocument(strings.Replace(block, "  allow_write: true\n", "", 1)), true},
		{"allow_write string", configDataDocument(strings.Replace(block, "allow_write: true", "allow_write: 'true'", 1)), false},
		{"allow_write int", configDataDocument(strings.Replace(block, "allow_write: true", "allow_write: 1", 1)), false},
		{"allow_write yes", configDataDocument(strings.Replace(block, "allow_write: true", "allow_write: yes", 1)), false},
		{"name bool", configDataDocument(strings.Replace(block, "name: models", "name: true", 1)), false},
		{"origin int", configDataDocument(strings.Replace(block, "origin: https://data.example.com:443", "origin: 443", 1)), false},
		{"unknown data key", configDataDocument(block + "proxy: none\n"), false},
		{"unknown reference key", configDataDocument(strings.Replace(block, "pvc: model-cache", "pvc: model-cache\n  sub_path: x", 1)), false},
		{"duplicate data key", configDataDocument(block + "origin: https://other.example.com\n"), false},
		{"anchor", configDataDocument(strings.Replace(block, "pvc: model-cache", "pvc: &claim model-cache", 1)), false},
		{"alias", configDataDocument(strings.Replace(block, "pvc: model-cache", "pvc: &c model-cache\n  secret: *c", 1)), false},
		{"null data", "version: 1\ntargets:\n- name: worker\n  namespace: jobs\n  in_cluster: true\n  data: ~\n", false},
		{"empty data", "version: 1\ntargets:\n- name: worker\n  namespace: jobs\n  in_cluster: true\n  data: {}\n", false},
		{"large ca", configDataDocument(configDataBlockCA(strings.Repeat(configDataPEM, 8))), true},
		{"oversize ca", configDataDocument(configDataBlockCA(strings.Repeat(configDataPEM, configMaxCA/len(configDataPEM)+1))), false},
		{"large scalar", configDataDocument(strings.Replace(block, "pvc: model-cache", "pvc: "+strings.Repeat("a", 4097), 1)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadConfig(configFile(t, test.text), false)
			if (err == nil) != test.ok {
				t.Fatalf("accepted=%v, want %v; %v", err == nil, test.ok, err)
			}
		})
	}
}

func TestDataConfigSnapshots(t *testing.T) {
	c, err := ReadConfig(configFile(t, configDataDocument(configDataBlock())), false)
	if err != nil {
		t.Fatal(err)
	}
	original := c.Targets()
	a := c.Targets()
	a[0].Data.HelperImage = "changed"
	a[0].Data.TaskImages[0] = "changed"
	a[0].Data.References[0].Destinations[0] = "changed"
	a[0].Data.References[1].Keys[0] = "changed"
	a[0].Data.References[1].AllowWrite = true
	a[0].Data.TaskImages = append(a[0].Data.TaskImages, "appended")
	if !reflect.DeepEqual(c.Targets(), original) {
		t.Fatal("returned data configuration changed frozen configuration")
	}
	legacy := Config{targets: []TargetConfig{{Name: "worker"}}}
	if legacy.Targets()[0].Data != nil {
		t.Fatal("nil data configuration became non-nil")
	}
	empty := Config{targets: []TargetConfig{{Name: "worker", Data: &DataConfig{}}}}
	if d := empty.Targets()[0].Data; d == nil || d.TaskImages != nil || d.References != nil {
		t.Fatal("nil data slices became non-nil")
	}
}

func ExampleDataConfig() {
	dir, err := os.MkdirTemp("", "skygo-data-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(path, []byte(configDataDocument(configDataBlock())), 0600); err != nil {
		panic(err)
	}
	config, err := ReadConfig(path, false)
	if err != nil {
		panic(err)
	}
	data := config.Targets()[0].Data
	fmt.Println(data.Origin, len(data.TaskImages))
	for _, r := range data.References {
		fmt.Println(r.Name, r.PVC+r.Secret, r.Keys, r.Destinations, r.AllowWrite)
	}
	// Output:
	// https://data.example.com:443 2
	// models model-cache [] [/mnt/skygo/models /mnt/skygo/cache] true
	// creds registry-creds [token ca.crt] [/mnt/skygo/creds] false
}

func ExampleDataReference() {
	r := DataReference{Name: "models", PVC: "model-cache", Destinations: []string{"/mnt/skygo/models"}}
	fmt.Println(r.Name, r.PVC, r.Destinations, r.AllowWrite)
	// Output: models model-cache [/mnt/skygo/models] false
}

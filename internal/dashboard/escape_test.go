package dashboard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
)

const hostile = `<script>alert(1)</script>"' onmouseover="x" javascript:alert(1)`

func TestVisible(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"text", "plain\ttext\n", "plain\ttext\n"},
		{"ansi", "\x1b[31mred\x1b[0m", "␛[31mred␛[0m"},
		{"nul", "a\x00b", "a␀b"},
		{"carriage return", "over\rwrite", "over␍write"},
		{"del", "a\x7fb", "a␡b"},
		{"c1", "a\u0085b\u009bc", `a\u0085b\u009bc`},
		{"invalid", "a\xff\xfeb", "a��b"},
		{"truncated rune", "a\xe2\x82", "a��"},
	}
	for _, tt := range tests {
		if got := visible([]byte(tt.in)); got != tt.want {
			t.Errorf("%s: visible(%q) = %q, want %q", tt.name, tt.in, got, tt.want)
		}
	}
}

var hrefs = regexp.MustCompile(`href="([^"]*)"`)

func TestEscaping(t *testing.T) {
	f := &fakeReader{
		// A hostile job ID reaches href attributes and text.
		jobs: []*v1.Job{{Id: "abc", Target: hostile}, {Id: hostile, Target: "local"}},
		job: &v1.Job{Id: "abc", Target: hostile, Reason: hostile, Task: &v1.Task{
			Argv:  []string{hostile, "javascript:alert(1)"},
			Image: hostile,
			Env:   map[string]string{"SECRET_NAME": "value-sentinel-" + hostile},
		}},
	}
	ts := newTestServer(t, f)
	c := ts.session(t)
	for _, path := range []string{"/jobs", "/jobs/abc", "/targets"} {
		w := ts.do(t, "GET", path, nil, c)
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("%s = %d %s", path, w.Code, w.Header().Get("Content-Type"))
		}
		for _, raw := range []string{"<script>", `" onmouseover`, `href="javascript:`} {
			if strings.Contains(body, raw) {
				t.Errorf("%s: unescaped %q in\n%s", path, raw, body)
			}
		}
		if !strings.Contains(body, "&lt;script&gt;") {
			t.Errorf("%s: hostile text missing escaped", path)
		}
		// Every link is a same-origin path: no scheme, no other host, and
		// no attribute break-out.
		links := hrefs.FindAllStringSubmatch(body, -1)
		if len(links) != strings.Count(body, "href=") {
			t.Errorf("%s: %d href attributes, %d well formed", path, strings.Count(body, "href="), len(links))
		}
		for _, l := range links {
			if !strings.HasPrefix(l[1], "/") || strings.HasPrefix(l[1], "//") || strings.ContainsAny(l[1], "<> ") {
				t.Errorf("%s: link %q is not an escaped same-origin path", path, l[1])
			}
		}
		if strings.Contains(body, "value-sentinel") || strings.Contains(body, ts.code.value) || strings.Contains(body, c.cookie.Value) || strings.Contains(body, c.proof) {
			t.Errorf("%s: secret disclosed", path)
		}
	}
	if w := ts.do(t, "GET", "/jobs/abc", nil, c); !strings.Contains(w.Body.String(), "SECRET_NAME") {
		t.Error("environment name not shown")
	}
	if w := ts.do(t, "GET", "/jobs", nil, c); !strings.Contains(w.Body.String(), `href="/jobs/`+url.PathEscape(hostile)+`"`) {
		t.Errorf("hostile job ID not path-escaped in its link:\n%s", w.Body)
	}
	if w := ts.do(t, "GET", "/jobs", nil, c); !strings.Contains(w.Body.String(), `href="/jobs?page_token=next%26token%3d%3cx%3e"`) && !strings.Contains(w.Body.String(), `href="/jobs?page_token=next%26token%3D%3Cx%3E"`) {
		t.Errorf("next link not query-escaped:\n%s", w.Body)
	}
}

func TestLogPageEscaping(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	w := httptest.NewRecorder()
	ts.render(w, http.StatusOK, "logs", "logs abc", logsView{ID: "abc", Job: "/jobs/abc", Text: "/jobs/abc/logs.txt", Output: visible([]byte("<script>alert(1)</script>\x1b[2J\x00\xff"))})
	body := w.Body.String()
	if strings.Contains(body, "<script>") || strings.Contains(body, "\x1b") || strings.Contains(body, "\x00") || !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;␛[2J␀�") {
		t.Errorf("log page body:\n%q", body)
	}
}

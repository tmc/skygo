package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

// spanRefEachMember is eachMember as it was before the member span walk: it
// decodes every member value with a json.Decoder. It is the oracle of the
// differential tests below.
func spanRefEachMember(data []byte, name string, fn func(value []byte) error) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if _, err := d.Token(); err != nil {
		return err
	}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := t.(string)
		if !ok {
			return errors.New("malformed stored record")
		}
		var v json.RawMessage
		if err := d.Decode(&v); err != nil {
			return err
		}
		if strings.EqualFold(key, name) {
			if err := fn(bytes.TrimSpace(v)); err != nil {
				return err
			}
		}
	}
	return nil
}

// spanRefStoredTask is storedTask as it was, over spanRefEachMember.
func spanRefStoredTask(data []byte, refusal error) error {
	if !json.Valid(data) {
		return errors.New("malformed stored record")
	}
	workers := false
	err := spanRefEachMember(data, "task", func(task []byte) error {
		return spanRefEachMember(task, "workers", func(w []byte) error {
			if !bytes.Equal(w, []byte("null")) {
				workers = true
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	if workers {
		return refusal
	}
	return nil
}

type spanEach func(data []byte, name string, fn func(value []byte) error) error

// A spanCall is one callback: the depth of its name and the value it was
// given, retained as given.
type spanCall struct {
	depth int
	value []byte
}

// spanRecord walks data with each, matching names[0] at the root, names[1]
// within each match, and so on, and returns every callback in order.
func spanRecord(each spanEach, data []byte, names []string) ([]spanCall, error) {
	var calls []spanCall
	var walk func(data []byte, depth int) error
	walk = func(data []byte, depth int) error {
		return each(data, names[depth], func(v []byte) error {
			calls = append(calls, spanCall{depth, v})
			if depth+1 < len(names) {
				return walk(v, depth+1)
			}
			return nil
		})
	}
	err := walk(data, 0)
	return calls, err
}

type spanInput struct {
	name string
	data []byte
}

func spanJobRaw(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(backend.Job{ID: "modeled", Task: workersPlain()})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func spanHeaderRaw(t *testing.T) []byte {
	t.Helper()
	task := workersPlain()
	raw, err := json.Marshal(cleanupHeader{Task: &task})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// spanEdited applies the edit of every workers case that holds for kind to
// raw, checking that each edit changes it.
func spanEdited(t *testing.T, kind string, raw []byte, header bool) []spanInput {
	t.Helper()
	var in []spanInput
	for _, tt := range workersCases {
		if header && tt.jobOnly {
			continue
		}
		data := bytes.Clone(raw)
		if tt.edit != nil {
			data = tt.edit(bytes.Clone(raw))
			if bytes.Equal(data, raw) {
				t.Fatalf("%s %s: edit changed nothing", kind, tt.name)
			}
		}
		in = append(in, spanInput{kind + "/" + tt.name, data})
	}
	return in
}

// spanSynthetic holds member walk boundaries: escaped delimiters in strings,
// nesting, escaped and Unicode keys, invalid UTF-8, duplicates, long values,
// shapes that are not objects, and malformed records.
func spanSynthetic() []spanInput {
	deep := `{"a":` + strings.Repeat(`[{"b":`, 2000) + `1` + strings.Repeat(`}]`, 2000) +
		`,"task":{"workers":` + strings.Repeat(`[`, 3000) + strings.Repeat(`]`, 3000) + `}}`
	long := `{"status":"` + strings.Repeat(`x\"\\}{][,:`, 1<<16) + `","codes":[` +
		strings.Repeat(`-1.5e+3,{"workers":1},"task",`, 1<<12) + `0],"task":{"workers":null},` +
		`"` + strings.Repeat("k", 1<<16) + `":1,"tail":{"task":{"workers":1}}}`
	var nums strings.Builder
	for i := range 1000 {
		nums.WriteString(`,"n` + strconv.Itoa(i) + `":` + strconv.Itoa(i))
	}
	return []spanInput{
		{"empty object", []byte(`{}`)},
		{"spaced empty object", []byte(" \n{ \t}\r\n")},
		{"empty task", []byte(`{"task":{}}`)},
		{"null workers", []byte(`{"task":{"workers":null}}`)},
		{"workers", []byte(`{"task":{"workers":1}}`)},
		{"white space", []byte(" \n\t{ \"task\" :\r\n{ \"workers\" : [ 1 , 2 ] , \"x\" : { } } , \"y\" : true }\n")},
		{"quoted member in string", []byte(`{"note":"\",\"task\":{\"workers\":1},\"x\":\"","task":{"argv":["work"]}}`)},
		{"quoted workers in task string", []byte(`{"task":{"b":"\"workers\":1,","workers":null,"c":"\\"}}`)},
		{"trailing backslash", []byte(`{"a":"\\","task":{"workers":2}}`)},
		{"backslash quote", []byte(`{"a":"\\\"","task":{"workers":null}}`)},
		{"backslash runs", []byte(`{"a":"\\\\\\\"}{][","task":{"b":"\\\\","workers":null},"c":"\\\\\""}`)},
		{"escaped quote keys", []byte(`{"ta\"sk":{"workers":1},"t\\ask":{"workers":2},"task\\":{"workers":3}}`)},
		{"delimiters in nested strings", []byte(`{"x":[{"y":"}]"},"]}",["{"]],"task":{"z":{"workers":1},"workers":false}}`)},
		{"upper case", []byte(`{"TASK":{"WORKERS":1}}`)},
		{"raw unicode fold keys", []byte("{\"ta\u017fk\":{\"wor\u212aers\":null},\"Ta\u017fK\":{\"\u212aorkers\":1}}")},
		{"escaped unicode fold keys", []byte(`{"ta\u017Fk":{"wor\u212Aers":1},"\u0074ask":{"workers":null}}`)},
		{"surrogates", []byte(`{"task\ud800":{"workers":1},"\ud83d\ude00":1,"task":{"\udc00workers":1,"\ud800":2}}`)},
		{"invalid utf8", []byte("{\"task\xff\":{\"workers\":1},\"t\xc3\":2,\"a\xff\":3,\"task\":{\"workers\xe2\x84\":3,\"x\":\"\xff\"}}")},
		{"fold names", []byte("{\"s\":1,\"\u017f\":2,\"S\":3,\"\\u017f\":4,\"k\":5,\"\u212a\":6,\"K\":7,\"a\ufffd\":8,\"a\xff\":9,\"\":10,\"\\ud800\":11,\"\\uFFFD\":12}")},
		{"duplicates", []byte(`{"task":{"workers":1},"task":null,"Task":{"workers":null,"workers":[],"WORKERS":null},"task":"x","task":{"workers":{"workers":1}},"task":[{"workers":1}]}`)},
		{"literals", []byte(`{"a":-0.5e+10,"b":true,"c":false,"d":null,"task":{"x":0,"workers":-1E-2},"e":12345678901234567890}`)},
		{"literal last", []byte(`{"task":{"workers":0} ,"z":7}`)},
		{"many members", []byte(`{"n":0` + nums.String() + `,"task":{"workers":null}}`)},
		{"deep", []byte(deep)},
		{"long", []byte(long)},

		{"null", []byte(`null`)},
		{"spaced null", []byte(`  null  `)},
		{"array", []byte(`[]`)},
		{"root array", []byte(`[{"task":{"workers":1}}]`)},
		{"string", []byte(`"task"`)},
		{"number", []byte(`1`)},
		{"true", []byte(`true`)},

		{"empty", []byte(``)},
		{"space", []byte(` `)},
		{"open", []byte(`{`)},
		{"unclosed", []byte(`{"task":{"workers":1}`)},
		{"trailing garbage", []byte(`{"task":{"workers":1}}x`)},
		{"trailing object", []byte(`{"task":{"workers":1}}{}`)},
		{"trailing comma", []byte(`{"task":{"workers":1}} ,`)},
		{"member comma", []byte(`{"a":1,}`)},
		{"array comma", []byte(`{"a":[1,]}`)},
		{"single quotes", []byte(`{'a':1}`)},
		{"no value", []byte(`{"a"}`)},
		{"missing workers value", []byte(`{"task":{"workers":}}`)},
		{"control character", []byte("{\"a\":\"\x01\"}")},
		{"short literal", []byte(`{"a":nul}`)},
		{"leading zero", []byte(`{"a":01}`)},
		{"bad escape", []byte(`{"a":"\x"}`)},
		{"mismatched", []byte(`{"task":{"workers":[1}}}`)},
	}
}

func spanCorpus(t *testing.T) []spanInput {
	t.Helper()
	in := spanEdited(t, "job", spanJobRaw(t), false)
	in = append(in, spanEdited(t, "header", spanHeaderRaw(t), true)...)
	return append(in, spanSynthetic()...)
}

var spanNames = [][]string{
	{"task", "workers"},
	{"Task"},
	{"workers"},
	{"s"},
	{"k"},
	{"a\ufffd"},
	{"\ufffd"},
	{""},
	{"task", "workers", "workers"},
	{"x", "y"},
}

func spanSameCalls(t *testing.T, label string, got, want []spanCall) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d callbacks, want %d", label, len(got), len(want))
		return
	}
	for i := range got {
		if got[i].depth != want[i].depth || !bytes.Equal(got[i].value, want[i].value) {
			t.Errorf("%s: callback %d: depth %d %.80q, want depth %d %.80q", label, i, got[i].depth, got[i].value, want[i].depth, want[i].value)
		}
	}
}

// TestEachMemberDifferential checks that eachMember gives every valid record
// the callbacks the decoder walk gave it, in order and byte for byte.
func TestEachMemberDifferential(t *testing.T) {
	valid := 0
	for _, in := range spanCorpus(t) {
		if !json.Valid(in.data) {
			continue
		}
		valid++
		for _, names := range spanNames {
			label := in.name + "/" + strings.Join(names, ".")
			want, wantErr := spanRecord(spanRefEachMember, in.data, names)
			got, err := spanRecord(eachMember, in.data, names)
			if err != nil || wantErr != nil {
				t.Errorf("%s: err %v, reference %v", label, err, wantErr)
			}
			spanSameCalls(t, label, got, want)
		}
	}
	if valid < 50 {
		t.Fatalf("only %d valid records", valid)
	}
}

// TestEachMemberMatches pins callbacks that a raw byte comparison of keys,
// or a walk that is fooled by escaped quotes, would get wrong.
func TestEachMemberMatches(t *testing.T) {
	for _, tt := range []struct {
		data string
		name string
		want []string
	}{
		{`{"note":"\",\"task\":1,\"x\":\"","task":2}`, "task", []string{"2"}},
		{`{"a":"\\","task":3}`, "task", []string{"3"}},
		{`{"ta\"sk":1,"t\\ask":2,"task":3}`, "task", []string{"3"}},
		{`{"ta\u0073k":1,"TASK":2,"tas\u212A":3,"ta\u017Fk":4,"tas":5}`, "task", []string{"1", "2", "3", "4"}},
		{"{\"a\xff\":1,\"a\\ufffd\":2,\"a\":3}", "a\ufffd", []string{"1", "2"}},
		{`{"\ud800":1,"\udc00\ud800":2,"\ud83d\ude00":3}`, "\ufffd", []string{"1"}},
		{`{"\ud83d\ude00":1}`, "\U0001F600", []string{"1"}},
		{`{"":1,"a":2,"":3}`, "", []string{"1", "3"}},
		{` { "x" : [ "]" , { "}" : 1 } ] , "x" : "{" } `, "x", []string{`[ "]" , { "}" : 1 } ]`, `"{"`}},
	} {
		var got []string
		if err := eachMember([]byte(tt.data), tt.name, func(v []byte) error {
			got = append(got, string(v))
			return nil
		}); err != nil {
			t.Errorf("%s: %v", tt.data, err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s %q: %q, want %q", tt.data, tt.name, got, tt.want)
		}
	}
}

// TestEachMemberCallbackError checks that a callback error stops the walk
// after the same callbacks as before and is returned as given.
func TestEachMemberCallbackError(t *testing.T) {
	stop := errors.New("stop")
	for _, in := range spanSynthetic() {
		if !json.Valid(in.data) {
			continue
		}
		for _, name := range []string{"task", "s"} {
			for n := 1; n <= 5; n++ {
				run := func(each spanEach) ([]string, error) {
					var calls []string
					err := each(in.data, name, func(v []byte) error {
						calls = append(calls, string(v))
						if len(calls) == n {
							return stop
						}
						return nil
					})
					return calls, err
				}
				want, wantErr := run(spanRefEachMember)
				got, err := run(eachMember)
				if err != wantErr || !reflect.DeepEqual(got, want) {
					t.Errorf("%s/%s/%d: %d callbacks %v, want %d %v", in.name, name, n, len(got), err, len(want), wantErr)
				}
			}
		}
	}
}

// TestEachMemberOwnership checks that each value is the callback's own:
// retaining, appending to or modifying one changes neither the input nor
// any other value, and modifying the input changes no value.
func TestEachMemberOwnership(t *testing.T) {
	data := []byte(`{"task":{"workers":[1]},"other":"x","TASK":{"workers":[2]}}`)
	orig := bytes.Clone(data)
	calls, err := spanRecord(eachMember, data, []string{"task", "workers"})
	if err != nil {
		t.Fatal(err)
	}
	want, err := spanRecord(spanRefEachMember, orig, []string{"task", "workers"})
	if err != nil {
		t.Fatal(err)
	}
	spanSameCalls(t, "retained", calls, want)
	if len(calls) != 4 {
		t.Fatalf("%d callbacks, want 4", len(calls))
	}
	snapshot := func() [][]byte {
		var s [][]byte
		for _, c := range calls {
			s = append(s, bytes.Clone(c.value))
		}
		return s
	}

	before := snapshot()
	for i := range calls[0].value {
		calls[0].value[i] = '#'
	}
	_ = append(calls[0].value[:1], "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"...)
	if !bytes.Equal(data, orig) {
		t.Fatalf("modifying the first value changed the input: %s", data)
	}
	after := snapshot()
	for i := 1; i < len(calls); i++ {
		if !bytes.Equal(after[i], before[i]) {
			t.Fatalf("modifying the first value changed value %d: %s", i, after[i])
		}
	}

	for i := range data {
		data[i] = '%'
	}
	for i := 1; i < len(calls); i++ {
		if !bytes.Equal(calls[i].value, before[i]) {
			t.Fatalf("modifying the input changed value %d: %s", i, calls[i].value)
		}
	}
}

// TestEachMemberPrefixes walks every proper prefix of each short valid
// object, which is not valid JSON. eachMember must not panic, must report
// it malformed, and may give only callbacks the decoder walk gave first.
func TestEachMemberPrefixes(t *testing.T) {
	for _, in := range spanCorpus(t) {
		data := bytes.TrimSpace(in.data)
		if len(data) > 512 || !json.Valid(data) || data[0] != '{' {
			continue
		}
		for n := 1; n < len(data); n++ {
			p := data[:n]
			if q := bytes.TrimSpace(p); len(q) == 0 || q[0] != '{' {
				continue
			}
			got, err := spanRecord(eachMember, p, []string{"task", "workers"})
			if err == nil {
				t.Errorf("%s[:%d]: accepted %q", in.name, n, p)
			}
			want, _ := spanRecord(spanRefEachMember, p, []string{"task", "workers"})
			if len(got) > len(want) {
				t.Errorf("%s[:%d]: %d callbacks, reference %d", in.name, n, len(got), len(want))
				continue
			}
			spanSameCalls(t, in.name+"[:"+strconv.Itoa(n)+"]", got, want[:len(got)])
		}
	}
}

// spanSameError checks that err is the error want: the same text, the same
// refusal identity and the same worker route.
func spanSameError(t *testing.T, label string, err, want, refusal error) {
	t.Helper()
	switch {
	case (err == nil) != (want == nil):
		t.Errorf("%s: err %v, want %v", label, err, want)
	case err == nil:
	case err.Error() != want.Error() || (err == refusal) != (want == refusal) || errors.Is(err, errWorkerRoute) != errors.Is(want, errWorkerRoute):
		t.Errorf("%s: err %v, want %v", label, err, want)
	}
}

// TestStoredTaskDifferential checks storedTask, including its refusal and
// malformed results, against the decoder walk over every record.
func TestStoredTaskDifferential(t *testing.T) {
	for _, in := range spanCorpus(t) {
		for _, refusal := range []error{errStoredJobWorkers, errStoredHeaderWorkers} {
			spanSameError(t, in.name, storedTask(in.data, refusal), spanRefStoredTask(in.data, refusal), refusal)
		}
	}
}

// spanWorkersWant reports the expected refusal of the workers case named by
// a corpus input, as workersCases states it.
func spanWorkersWant(name string) (string, bool) {
	_, tt, ok := strings.Cut(name, "/")
	if !ok {
		return "", false
	}
	for _, c := range workersCases {
		if c.name == tt {
			return c.want, true
		}
	}
	return "", false
}

// TestDecodeStoredJobDifferential checks decodeStoredJob against the old
// guard followed by the same typed decode: the same error and the same job.
// For each workers case it also checks the refusal workersCases states.
func TestDecodeStoredJobDifferential(t *testing.T) {
	for _, in := range spanCorpus(t) {
		var got, want backend.Job
		err := decodeStoredJob(in.data, &got)
		wantErr := spanRefStoredTask(in.data, errStoredJobWorkers)
		if wantErr == nil {
			wantErr = json.Unmarshal(in.data, &want)
		}
		spanSameError(t, in.name, err, wantErr, errStoredJobWorkers)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: job %+v, want %+v", in.name, got, want)
		}
		if strings.HasPrefix(in.name, "job/") {
			spanOracle(t, in.name, err)
		}
	}
}

// TestDecodeCleanupHeaderDifferential is TestDecodeStoredJobDifferential for
// decodeCleanupHeader.
func TestDecodeCleanupHeaderDifferential(t *testing.T) {
	for _, in := range spanCorpus(t) {
		var got, want cleanupHeader
		err := decodeCleanupHeader(in.data, &got)
		wantErr := spanRefStoredTask(in.data, errStoredHeaderWorkers)
		if wantErr == nil {
			wantErr = json.Unmarshal(in.data, &want)
		}
		spanSameError(t, in.name, err, wantErr, errStoredHeaderWorkers)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: header %+v, want %+v", in.name, got, want)
		}
		if strings.HasPrefix(in.name, "header/") {
			spanOracle(t, in.name, err)
		}
	}
}

// spanOracle checks err against the refusal workersCases states for the
// case named by name.
func spanOracle(t *testing.T, name string, err error) {
	t.Helper()
	want, ok := spanWorkersWant(name)
	if !ok {
		t.Fatalf("%s: no workers case", name)
	}
	if want == "" {
		if err != nil {
			t.Errorf("%s: %v, want accepted", name, err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%s: %v, want %q", name, err, want)
	}
}

package tasklower_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/integrations/skycompat/tasklower"
	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
	core "github.com/tmc/skygo/skycompat/tasklower"
)

func TestPublicEntryPoint(t *testing.T) {
	d, err := taskyaml.Parse(strings.NewReader("run: echo hello\nresources: {cpus: 2+}\n"), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	opts := tasklower.Options{Shell: "/bin/sh"}
	got, notes, err := tasklower.Lower(d, opts)
	if err != nil {
		t.Fatal(err)
	}
	want, wantNotes, err := core.Lower(d, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(notes, wantNotes) {
		t.Fatal("public lowering differs from core")
	}
	data, err := tasklower.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantData, err := core.Marshal(want)
	if err != nil || !bytes.Equal(data, wantData) {
		t.Fatal("public encoding differs from core")
	}
	d, err = taskyaml.Parse(strings.NewReader("run: echo hello\nresources: {autostop: 10m}\n"), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = tasklower.Lower(d, opts)
	var field *taskyaml.Error
	if !errors.As(err, &field) || field.Code != taskyaml.Unsupported || field.Path != "resources.autostop" {
		t.Fatalf("typed refusal: %v", err)
	}
}

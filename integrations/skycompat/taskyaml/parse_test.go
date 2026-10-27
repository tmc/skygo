package taskyaml_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
	core "github.com/tmc/skygo/skycompat/taskyaml"
)

func TestPublicEntryPoint(t *testing.T) {
	const text = "resources:\n  autostop: {idle_minutes: 10, down: true}\n  accelerators: {L4:1, A10G:1}\n  infra: ssh/dgx\n  disk_size: 128\n  job_recovery: {strategy: EAGER_NEXT_REGION, recover_on_exit_codes: [1, 2]}\nrun: echo hello\n"
	got, err := taskyaml.Parse(strings.NewReader(text), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want, err := core.Parse(strings.NewReader(text), core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public document differs from core: %+v", got)
	}
	// Error aliases preserve errors.As across the module boundary.
	_, err = taskyaml.Parse(strings.NewReader("resources: {unknown: true}"), taskyaml.Options{})
	var public *taskyaml.Error
	var canonical *core.Error
	if !errors.As(err, &public) || !errors.As(err, &canonical) || public != canonical {
		t.Fatalf("error identity: %v", err)
	}
}

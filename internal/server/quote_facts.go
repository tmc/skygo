package server

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
	"gopkg.in/yaml.v3"
)

const maxTargetFactsBytes = 64 << 10

// Startup DTOs preserve YAML spelling. Semantic validation is shared with
// direct Go configuration through backend.ValidateAdvice.
type configuredTargetFacts struct {
	Name     string                  `yaml:"name"`
	Quote    *configuredQuoteFact    `yaml:"quote"`
	Capacity *configuredCapacityFact `yaml:"capacity"`
}

type configuredQuoteFact struct {
	AmountMicros *int64 `yaml:"amount_micros"`
	Currency     string `yaml:"currency"`
	Unit         string `yaml:"unit"`
	Source       string `yaml:"source"`
	ObservedAt   string `yaml:"observed_at"`
	ValidUntil   string `yaml:"valid_until"`
}

type configuredCapacityFact struct {
	State          string  `yaml:"state"`
	AvailableSlots *uint32 `yaml:"available_slots"`
	Source         string  `yaml:"source"`
	ObservedAt     string  `yaml:"observed_at"`
	ValidUntil     string  `yaml:"valid_until"`
}

// readTargetFacts reads only startup metadata; planning never calls it.
func readTargetFacts(path string) (map[string]*backend.TargetAdvice, error) {
	return readTargetFactsWithOpen(path, openTargetFacts)
}

func readTargetFactsWithOpen(path string, open func(string) (*os.File, error)) (map[string]*backend.TargetAdvice, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("read target facts")
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("target facts must be a regular file")
	}
	f, err := open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, errors.New("target facts file changed")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxTargetFactsBytes+1))
	if err != nil {
		return nil, errors.New("read target facts")
	}
	return parseTargetFacts(data)
}

func parseTargetFacts(data []byte) (map[string]*backend.TargetAdvice, error) {
	if len(data) > maxTargetFactsBytes {
		return nil, errors.New("target facts exceed 64 KiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var tree yaml.Node
	if err := decoder.Decode(&tree); err != nil || len(tree.Content) != 1 || tree.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("invalid target facts syntax")
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("target facts must contain one document")
	}
	nodes := 0
	if err := checkFactsNode(&tree, "", 0, &nodes); err != nil {
		return nil, err
	}
	var doc struct {
		Version uint32                  `yaml:"version"`
		Targets []configuredTargetFacts `yaml:"targets"`
	}
	decoder = yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return nil, errors.New("unknown field or invalid value in target facts")
	}
	if doc.Version != 1 {
		return nil, errors.New("unsupported target facts version")
	}
	if len(doc.Targets) == 0 || len(doc.Targets) > 32 {
		return nil, errors.New("target facts require 1..32 targets")
	}
	facts := make(map[string]*backend.TargetAdvice, len(doc.Targets))
	for _, target := range doc.Targets {
		if target.Name == "" || len(target.Name) > 256 || !utf8.ValidString(target.Name) {
			return nil, errors.New("invalid target facts name")
		}
		if _, exists := facts[target.Name]; exists {
			return nil, errors.New("duplicate target facts name")
		}
		advice, err := configuredAdvice(target)
		if err != nil {
			return nil, err
		}
		facts[target.Name] = advice
	}
	return facts, nil
}

func checkFactsNode(n *yaml.Node, key string, depth int, nodes *int) error {
	*nodes += 1
	if *nodes > 2048 || depth > 8 {
		return errors.New("target facts nesting or node limit exceeded")
	}
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return errors.New("target facts aliases and anchors are unsupported")
	}
	switch n.Kind {
	case yaml.DocumentNode:
		for _, child := range n.Content {
			if err := checkFactsNode(child, key, depth+1, nodes); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		if n.Tag != "!!map" {
			return errors.New("invalid target facts tag")
		}
		seen := make(map[string]bool)
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			*nodes += 1
			if *nodes > 2048 {
				return errors.New("target facts nesting or node limit exceeded")
			}
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || k.Anchor != "" || len(k.Value) > 256 || !utf8.ValidString(k.Value) || strings.ContainsRune(k.Value, 0) {
				return errors.New("invalid target facts key")
			}
			if seen[k.Value] {
				return errors.New("duplicate target facts key")
			}
			seen[k.Value] = true
			if err := checkFactsNode(n.Content[i+1], k.Value, depth+1, nodes); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		if n.Tag != "!!seq" {
			return errors.New("invalid target facts tag")
		}
		for _, child := range n.Content {
			if err := checkFactsNode(child, key, depth+1, nodes); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		switch key {
		case "version", "amount_micros", "available_slots":
			if n.Tag != "!!int" || !factsDecimal(n.Value) {
				return errors.New("invalid target facts integer")
			}
		case "observed_at", "valid_until":
			if n.Tag != "!!str" && n.Tag != "!!timestamp" {
				return errors.New("invalid target facts timestamp")
			}
			if _, err := factsTime(n.Value); err != nil {
				return err
			}
		default:
			if n.Tag != "!!str" || !utf8.ValidString(n.Value) || strings.ContainsRune(n.Value, 0) {
				return errors.New("invalid target facts scalar")
			}
		}
	default:
		return errors.New("invalid target facts node")
	}
	return nil
}

func factsDecimal(s string) bool {
	if len(s) == 0 || len(s) > 20 || len(s) > 1 && s[0] == '0' {
		return false
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func factsTime(s string) (time.Time, error) {
	if !factsTimeSyntax(s) {
		return time.Time{}, errors.New("invalid target facts timestamp")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.IsZero() || t.Year() < 1 || t.Year() > 9999 {
		return time.Time{}, errors.New("invalid target facts timestamp")
	}
	return t.UTC(), nil
}

// factsTimeSyntax checks the UTC RFC3339Nano spelling. time.Parse also accepts
// comma fractions and unpadded hours, which are not configured facts syntax.
func factsTimeSyntax(s string) bool {
	if len(s) < 20 || len(s) > 30 || s[len(s)-1] != 'Z' {
		return false
	}
	for i := 0; i < 19; i++ {
		switch i {
		case 4, 7:
			if s[i] != '-' {
				return false
			}
		case 10:
			if s[i] != 'T' {
				return false
			}
		case 13, 16:
			if s[i] != ':' {
				return false
			}
		default:
			if s[i] < '0' || s[i] > '9' {
				return false
			}
		}
	}
	if len(s) == 20 {
		return true
	}
	if len(s) < 22 || s[19] != '.' {
		return false
	}
	for i := 20; i < len(s)-1; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func configuredAdvice(t configuredTargetFacts) (*backend.TargetAdvice, error) {
	a := &backend.TargetAdvice{}
	if p := t.Quote; p != nil {
		observed, err := factsTime(p.ObservedAt)
		if err != nil {
			return nil, err
		}
		until, err := factsTime(p.ValidUntil)
		if err != nil {
			return nil, err
		}
		unit := map[string]backend.RateUnit{"target-hour": backend.TargetHour, "cpu-hour": backend.CPUHour, "accelerator-hour": backend.AcceleratorHour}[p.Unit]
		a.Quote = &backend.Quote{Rate: backend.Money{AmountMicros: p.AmountMicros, Currency: p.Currency}, Unit: unit, Origin: backend.Configured, Source: p.Source, ObservedAt: observed, ValidUntil: until}
	}
	if c := t.Capacity; c != nil {
		observed, err := factsTime(c.ObservedAt)
		if err != nil {
			return nil, err
		}
		until, err := factsTime(c.ValidUntil)
		if err != nil {
			return nil, err
		}
		state := map[string]backend.CapacityState{"available": backend.CapacityAvailable, "unavailable": backend.CapacityUnavailable, "error": backend.CapacityError}[c.State]
		a.Capacity = &backend.CapacitySnapshot{State: state, AvailableSlots: c.AvailableSlots, Origin: backend.Configured, Source: c.Source, ObservedAt: observed, ValidUntil: until}
	}
	if err := backend.ValidateAdvice(a); err != nil {
		return nil, err
	}
	return backend.CloneAdvice(a), nil
}

// validateTargetFactNames runs after target construction, before trusted migration.
func validateTargetFactNames(facts map[string]*backend.TargetAdvice, targets []backend.Target) error {
	names := make(map[string]bool, len(targets))
	for _, target := range targets {
		names[target.Name] = true
	}
	for name := range facts {
		if !names[name] {
			return errors.New("target facts name is not configured")
		}
	}
	return nil
}

func attachTargetFacts(facts map[string]*backend.TargetAdvice, targets []backend.Target) error {
	if err := validateTargetFactNames(facts, targets); err != nil {
		return err
	}
	for i := range targets {
		if advice, ok := facts[targets[i].Name]; ok {
			targets[i].Advice = backend.CloneAdvice(advice)
		}
	}
	return nil
}

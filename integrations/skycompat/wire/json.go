package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"unicode/utf8"
)

const (
	maxBody   = 1 << 20
	maxDAG    = 512 << 10
	maxResult = 256 << 10
	maxDepth  = 32
	maxTokens = 65536
)

// DecodeError identifies a rejected body without including client data.
// Code is invalid, unsupported, or limit. Field is a recognized schema field
// or empty; unknown field names and supplied values are never included.
type DecodeError struct {
	Code  string
	Field string
}

func (e *DecodeError) Error() string {
	if e.Field == "" {
		return "wire: " + e.Code + " request body"
	}
	return "wire: " + e.Code + " " + e.Field
}

func invalid(field string) error     { return &DecodeError{Code: "invalid", Field: field} }
func unsupported(field string) error { return &DecodeError{Code: "unsupported", Field: field} }
func limit(field string) error       { return &DecodeError{Code: "limit", Field: field} }

func readJSON(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, invalid("")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, errors.New("wire: read request body")
	}
	if len(data) > maxBody {
		return nil, limit("")
	}
	if err := checkJSON(data, maxDAG); err != nil {
		return nil, err
	}
	return data, nil
}

func checkJSON(data []byte, maxString int) error {
	if !utf8.Valid(data) || !validEscapes(data) {
		return invalid("")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	tokens := 0
	if err := jsonValue(d, 0, maxString, &tokens); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return invalid("")
	}
	return nil
}

func jsonValue(d *json.Decoder, depth, maxString int, tokens *int) error {
	if depth > maxDepth {
		return limit("")
	}
	*tokens += 1
	if *tokens > maxTokens {
		return limit("")
	}
	t, err := d.Token()
	if err != nil {
		return invalid("")
	}
	switch v := t.(type) {
	case string:
		if len(v) > maxString {
			return limit("")
		}
	case json.Number:
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
			return invalid("")
		}
	case json.Delim:
		if v != '{' && v != '[' {
			return invalid("")
		}
		keys := make(map[string]bool)
		for d.More() {
			if v == '{' {
				key, err := d.Token()
				name, ok := key.(string)
				if err != nil || !ok || keys[name] {
					return invalid("")
				}
				if len(name) > maxString {
					return limit("")
				}
				keys[name] = true
				*tokens += 1
			}
			if err := jsonValue(d, depth+1, maxString, tokens); err != nil {
				return err
			}
		}
		*tokens += 1
		if *tokens > maxTokens {
			return limit("")
		}
		end, err := d.Token()
		if err != nil || (v == '{' && end != json.Delim('}')) || (v == '[' && end != json.Delim(']')) {
			return invalid("")
		}
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 surrogates. Refuse that lossy input.
func validEscapes(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

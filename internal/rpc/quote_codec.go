package rpc

import (
	"bytes"
	"encoding/json"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxQuoteResponseBytes = 64 << 10

// marshalQuoteJSON is the compact representation used by the QuoteTask-only
// codec. The CLI uses the same options with protobuf field names.
func marshalQuoteJSON(m proto.Message, protoNames bool) ([]byte, error) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil, errors.New("invalid quote response")
	}
	b, err := (protojson.MarshalOptions{UseProtoNames: protoNames}).Marshal(m)
	if err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, b); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
}

// checkQuoteResponseSize runs in the QuoteTask handler before return,
// not in Codec.Marshal: Connect wraps codec marshal failures as Internal.
func checkQuoteResponseSize(m proto.Message) error {
	if m == nil || !m.ProtoReflect().IsValid() {
		return connect.NewError(connect.CodeInternal, errors.New("quote response is invalid"))
	}
	if proto.Size(m) > maxQuoteResponseBytes {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("quote response exceeds 64 KiB"))
	}
	for _, protoNames := range []bool{false, true} {
		b, err := marshalQuoteJSON(m, protoNames)
		if err != nil {
			return connect.NewError(connect.CodeInternal, errors.New("quote response is invalid"))
		}
		if len(b) > maxQuoteResponseBytes {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("quote response exceeds 64 KiB"))
		}
	}
	return nil
}

// quoteJSONCodec is registered only for QuoteTask; existing codecs are unchanged.
type quoteJSONCodec struct{ name string }

func (c quoteJSONCodec) Name() string { return c.name }

func (c quoteJSONCodec) Marshal(value any) ([]byte, error) {
	m, ok := value.(proto.Message)
	if !ok {
		return nil, errors.New("invalid quote message")
	}
	return marshalQuoteJSON(m, false)
}

func (c quoteJSONCodec) Unmarshal(data []byte, value any) error {
	m, ok := value.(proto.Message)
	if !ok || m == nil || !m.ProtoReflect().IsValid() {
		return errors.New("invalid quote message")
	}
	if len(data) == 0 {
		return errors.New("zero-length payload is not a valid JSON object")
	}
	return (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, m)
}

package skyv1_test

import (
	"encoding/hex"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestRetentionWireVectors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message proto.Message
		hex     string
	}{
		{"domain", &v1.RequestDomain{Lineage: strings.Repeat("1", 32), Epoch: 7}, "0a2031313131313131313131313131313131313131313131313131313131313131311007"},
		{"expiry", &v1.RetentionExpiry{Reason: v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED, JobId: strings.Repeat("2", 32)}, "080112203232323232323232323232323232323232323232323232323232323232323232"},
		{"capacity", &v1.RetentionCapacity{Domain: v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_OUTPUT_BYTES, Used: 1064960, Limit: 268435456}, "080910808041188080808001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(data); got != tc.hex {
				t.Fatalf("wire = %s, want %s", got, tc.hex)
			}
			decoded := tc.message.ProtoReflect().Type().New().Interface()
			if err := proto.Unmarshal(data, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, tc.message) {
				t.Fatal("binary round trip changed message")
			}
			text, err := protojson.Marshal(tc.message)
			if err != nil {
				t.Fatal(err)
			}
			proto.Reset(decoded)
			if err := protojson.Unmarshal(text, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, tc.message) {
				t.Fatal("JSON round trip changed message")
			}
		})
	}
}

func TestRetentionMetricPresence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   *uint64
		present bool
	}{
		{"unavailable", nil, false}, {"measured_zero", proto.Uint64(0), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := &v1.RetentionAdmin{DatabaseFileBytes: tc.value, DatabaseAllocatedBytes: tc.value, FilesystemFreeBytes: tc.value, OutputAllocatedBytes: tc.value, OutputFileCount: tc.value}
			data, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			var decoded v1.RetentionAdmin
			if err := proto.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"database_file_bytes", "database_allocated_bytes", "filesystem_free_bytes", "output_allocated_bytes", "output_file_count"} {
				descriptor := decoded.ProtoReflect().Descriptor().Fields().ByTextName(field)
				if decoded.ProtoReflect().Has(descriptor) != tc.present {
					t.Fatalf("%s presence changed", field)
				}
			}
			text, err := protojson.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(text), "databaseFileBytes") != tc.present {
				t.Fatalf("JSON lost metric presence: %s", text)
			}
		})
	}
}

func TestRetentionWireRejectsInvalidUTF8(t *testing.T) {
	message := &v1.RequestDomain{Lineage: string([]byte{0xc3, 0x28}), Epoch: 1}
	if _, err := proto.Marshal(message); err == nil {
		t.Fatal("binary marshal accepted invalid UTF8")
	}
	if _, err := protojson.Marshal(message); err == nil {
		t.Fatal("JSON marshal accepted invalid UTF8")
	}
	// Field1, two invalid UTF8 bytes: decoding must not silently repair identity.
	if err := proto.Unmarshal([]byte{0x0a, 0x02, 0xc3, 0x28}, &v1.RequestDomain{}); err == nil {
		t.Fatal("binary unmarshal accepted invalid UTF8")
	}
}

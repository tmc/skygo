package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

func parseRequestDomain(text string) (*v1.RequestDomain, error) {
	if !utf8.ValidString(text) || len(text) < 34 || len(text) > 53 {
		return nil, errors.New("request domain requires LINEAGE:EPOCH")
	}
	lineage, number, ok := strings.Cut(text, ":")
	if !ok || !validHexID(lineage) {
		return nil, errors.New("request domain requires a lowercase hexadecimal lineage")
	}
	epoch, err := strconv.ParseUint(number, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != number {
		return nil, errors.New("request domain requires a positive canonical epoch")
	}
	return &v1.RequestDomain{Lineage: lineage, Epoch: epoch}, nil
}

func validHexID(text string) bool {
	if len(text) != 32 {
		return false
	}
	for _, c := range text {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func domainText(domain *v1.RequestDomain) (string, error) {
	if domain == nil {
		return "", errors.New("server returned no request domain")
	}
	text := domain.Lineage + ":" + strconv.FormatUint(domain.Epoch, 10)
	if _, err := parseRequestDomain(text); err != nil {
		return "", errors.New("server returned an invalid request domain")
	}
	return text, nil
}

func (o *options) submitTask(cmd *cobra.Command, ctx context.Context, client skyv1connect.ControlServiceClient, task *v1.Task, id, domain, file string, domainSet, suppliedID bool) (*v1.Job, error) {
	if (suppliedID && id == "") || (id != "" && !validRequestID(id)) {
		return nil, errors.New("request ID requires 1..128 letters, digits, underscores or hyphens")
	}
	var selected *v1.RequestDomain
	var enabled bool
	if domainSet {
		var err error
		selected, err = parseRequestDomain(domain)
		if err != nil {
			return nil, err
		}
	} else {
		info, err := client.GetRetention(ctx, request(o, &v1.GetRetentionRequest{}))
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			return nil, err
		}
		if err == nil {
			if info == nil || info.Msg == nil {
				return nil, errors.New("server returned no retention information")
			}
			enabled = info.Msg.Enabled
			if enabled && !suppliedID {
				var err error
				domain, err = domainText(info.Msg.Domain)
				if err != nil {
					return nil, err
				}
				if info.Msg.AcceptUntil == nil || info.Msg.AcceptUntil.CheckValid() != nil || !time.Now().Before(info.Msg.AcceptUntil.AsTime()) {
					return nil, errors.New("server returned no accepting request domain")
				}
				selected, _ = parseRequestDomain(domain)
			}
		}
	}
	retrying, err := enabledRetry(task)
	if err != nil {
		return nil, err
	}
	// Retained replay and enabled legacy lookup precede current placement. Only
	// disabled/old-server default commands retain S05's extension preflight.
	if selected == nil && !enabled && !retrying && len(taskCapabilities(task)) != 0 {
		if _, err := o.planTask(ctx, client, task); err != nil {
			return nil, err
		}
	}
	if id == "" {
		var data [16]byte
		if _, err := rand.Read(data[:]); err != nil {
			return nil, err
		}
		id = hex.EncodeToString(data[:])
	}
	if selected != nil {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-domain: %s\nrequest-id: %s\ntask-file: %q\n", domain, id, file); err != nil {
			return nil, fmt.Errorf("write request hint: %w", err)
		}
		response, err := client.SubmitRetainedJob(ctx, request(o, &v1.SubmitRetainedJobRequest{Task: task, RequestId: id, Domain: selected}))
		if err != nil {
			return nil, fmt.Errorf("submit (--request-domain %s --request-id %s): %w", domain, id, retentionDiagnostic(err))
		}
		if response == nil || response.Msg == nil || response.Msg.Job == nil {
			return nil, errors.New("server returned no job")
		}
		return response.Msg.Job, nil
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "request-id: %s\n", id); err != nil {
		return nil, fmt.Errorf("write request id: %w", err)
	}
	submit := client.SubmitJob
	if retrying {
		submit = client.SubmitRetryJob
	}
	response, err := submit(ctx, request(o, &v1.SubmitJobRequest{Task: task, RequestId: id}))
	if err != nil {
		return nil, fmt.Errorf("submit (retry with --request-id %s): %w", id, retentionDiagnostic(err))
	}
	if response == nil || response.Msg == nil || response.Msg.Job == nil {
		return nil, errors.New("server returned no job")
	}
	return response.Msg.Job, nil
}

func retentionDiagnostic(err error) error {
	var rpc *connect.Error
	if !errors.As(err, &rpc) {
		return err
	}
	for _, detail := range rpc.Details() {
		value, decodeErr := detail.Value()
		if decodeErr != nil {
			continue
		}
		switch value := value.(type) {
		case *v1.RetentionExpiry:
			reason := ""
			switch value.Reason {
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED:
				reason = "result expired"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED:
				reason = "request epoch expired"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED:
				reason = "explicit request domain required"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED:
				reason = "request lineage changed"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_LOGS_EXPIRED:
				reason = "logs expired"
			}
			if reason != "" {
				return fmt.Errorf("%s: %w", reason, err)
			}
		case *v1.RetentionCapacity:
			return fmt.Errorf("retention capacity %s used=%d limit=%d: %w", value.Domain, value.Used, value.Limit, err)
		}
	}
	return err
}

func validRequestID(text string) bool {
	if len(text) < 1 || len(text) > 128 {
		return false
	}
	for _, c := range text {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

package cli

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/proto"
)

func (o *options) readLogs(cmd *cobra.Command, id string, offset int64, attempt uint32, follow bool, cursor string, cursorSet bool) (result error) {
	if offset < 0 || offset > backend.MaxLogBytes {
		return errors.New("offset must be between 0 and 1048576")
	}
	if attempt > 10 || (cmd.Flags().Changed("attempt") && attempt == 0) {
		return errors.New("attempt must be between 1 and 10")
	}
	if len(cursor) > 683 || !utf8.ValidString(cursor) || (cursor != "" && offset != 0) {
		return errors.New("invalid log cursor or conflicting offset")
	}
	ctx := cmd.Context()
	if !follow {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}
	client := o.client()
	retained := cursorSet
	if !cursorSet {
		checkctx, cancel := context.WithTimeout(ctx, o.timeout)
		info, err := client.GetRetention(checkctx, request(o, &v1.GetRetentionRequest{}))
		cancel()
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			return retentionDiagnostic(err)
		}
		if err == nil {
			if info == nil || info.Msg == nil {
				return errors.New("server returned no retention information")
			}
			retained = info.Msg.Enabled
		}
	}
	warned := false
	if retained {
		stream, err := client.StreamRetainedJobLogs(ctx, request(o, &v1.StreamRetainedJobLogsRequest{Id: id, Offset: offset, Attempt: attempt, Follow: follow, Cursor: cursor}))
		if err != nil {
			return retentionDiagnostic(err)
		}
		defer stream.Close()
		last, selected := "", ""
		defer func() {
			if last != "" && !o.json {
				_, err := fmt.Fprintf(cmd.ErrOrStderr(), "log-cursor: %s\n", last)
				if result == nil {
					result = err
				}
			}
		}()
		for stream.Receive() {
			message := stream.Msg()
			if !validHexID(message.AttemptId) || (selected != "" && selected != message.AttemptId) || message.Cursor == "" || len(message.Cursor) > 683 || !utf8.ValidString(message.Cursor) {
				return errors.New("server returned an invalid fixed log domain")
			}
			selected = message.AttemptId
			if err := o.writeLogChunk(cmd, message.Data, message.Truncated, message, &warned); err != nil {
				return err
			}
			last = message.Cursor
		}
		return retentionDiagnostic(stream.Err())
	}
	if attempt > 1 {
		checkctx, cancel := context.WithTimeout(ctx, o.timeout)
		response, err := client.GetJob(checkctx, request(o, &v1.GetJobRequest{Id: id}))
		cancel()
		if err != nil {
			return retentionDiagnostic(err)
		}
		if response == nil || response.Msg == nil || response.Msg.Job == nil {
			return errors.New("server returned no job")
		}
		if attempt > uint32(len(response.Msg.Job.Attempts)) {
			return errors.New("server returned no such attempt")
		}
	}
	stream, err := client.StreamJobLogs(ctx, request(o, &v1.StreamJobLogsRequest{Id: id, Offset: offset, Follow: follow, Attempt: attempt}))
	if err != nil {
		return retentionDiagnostic(err)
	}
	defer stream.Close()
	for stream.Receive() {
		message := stream.Msg()
		if err := o.writeLogChunk(cmd, message.Data, message.Truncated, message, &warned); err != nil {
			return err
		}
	}
	return retentionDiagnostic(stream.Err())
}

func (o *options) writeLogChunk(cmd *cobra.Command, data []byte, truncated bool, message proto.Message, warned *bool) error {
	if truncated && !*warned {
		if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "logs truncated: retained output limit reached"); err != nil {
			return err
		}
		*warned = true
	}
	if o.json {
		return writeJSON(cmd.OutOrStdout(), message)
	}
	_, err := cmd.OutOrStdout().Write(data)
	return err
}

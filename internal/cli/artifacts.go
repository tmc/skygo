package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
)

const artifactChunkSize = 64 << 10

func (o *options) artifactsCommand() *cobra.Command {
	c := &cobra.Command{Use: "artifacts", Short: "Transfer bounded directory snapshots"}
	var resume string
	upload := &cobra.Command{Use: "upload DIRECTORY", Short: "Upload a quiescent directory; write a Ref JSON object", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("resume") && !validHexID(resume) {
			return errors.New("resume requires an upload ID")
		}
		return o.uploadArtifact(cmd, args[0], resume)
	}}
	upload.Flags().StringVar(&resume, "resume", "", "Resume this upload of exactly the same archive")
	status := &cobra.Command{Use: "status UPLOAD_ID", Short: "Show an upload's acknowledged offset", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !validHexID(args[0]) {
			return errors.New("invalid upload ID")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1); err != nil {
			return err
		}
		r, err := o.artifactClient().GetArtifactUpload(ctx, request(o, &v1.GetArtifactUploadRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		if r == nil || r.Msg == nil || r.Msg.Upload == nil {
			return errors.New("server returned no upload")
		}
		u := r.Msg.Upload
		if err := checkUpload(u, args[0], nil); err != nil {
			return err
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), u)
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%d/%d\n", u.Id, u.State.String(), u.Acknowledged, u.Reference.Size)
		return err
	}}
	var cancelYes bool
	cancelUpload := &cobra.Command{Use: "cancel UPLOAD_ID --yes", Short: "Cancel an upload", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !cancelYes || !validHexID(args[0]) {
			return errors.New("cancel requires an upload ID and --yes")
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1); err != nil {
			return err
		}
		r, err := o.artifactClient().CancelArtifactUpload(ctx, request(o, &v1.CancelArtifactUploadRequest{Id: args[0]}))
		if err != nil {
			return err
		}
		if r == nil || r.Msg == nil {
			return errors.New("server returned no cancellation response")
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), args[0])
		return err
	}}
	cancelUpload.Flags().BoolVar(&cancelYes, "yes", false, "Confirm upload cancellation")
	var downloadRef string
	download := &cobra.Command{Use: "download --ref REF.json DEST", Short: "Verify and extract a snapshot into a new private directory", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ref, err := readArtifactRef(downloadRef)
		if err != nil {
			return err
		}
		return o.downloadArtifact(cmd, ref, args[0])
	}}
	download.Flags().StringVar(&downloadRef, "ref", "", "Ref JSON file from upload or a job result")
	var removeRef string
	var removeYes bool
	remove := &cobra.Command{Use: "remove --ref REF.json --yes", Short: "Remove an exact snapshot under server authority", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if !removeYes {
			return errors.New("remove requires --yes")
		}
		ref, err := readArtifactRef(removeRef)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		defer cancel()
		if _, err := o.requireEndpointCapabilities(ctx, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1); err != nil {
			return err
		}
		r, err := o.artifactClient().RemoveArtifact(ctx, request(o, &v1.RemoveArtifactRequest{Reference: wireArtifactRef(ref)}))
		if err != nil {
			return err
		}
		if r == nil || r.Msg == nil {
			return errors.New("server returned no removal response")
		}
		if o.json {
			return writeJSON(cmd.OutOrStdout(), r.Msg)
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), ref.ID)
		return err
	}}
	remove.Flags().StringVar(&removeRef, "ref", "", "Ref JSON file identifying the exact snapshot")
	remove.Flags().BoolVar(&removeYes, "yes", false, "Confirm snapshot removal")
	c.AddCommand(upload, status, cancelUpload, download, remove)
	return c
}

func readArtifactRef(path string) (artifact.Ref, error) {
	var ref artifact.Ref
	if path == "" {
		return ref, errors.New("artifact requires --ref")
	}
	f, err := os.Open(path)
	if err != nil {
		return ref, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return ref, err
	}
	if len(b) > 4096 {
		return ref, errors.New("artifact Ref exceeds 4 KiB")
	}
	// Decode tokens explicitly to reject duplicate keys as well as unknown keys.
	d := json.NewDecoder(bytes.NewReader(b))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return ref, errors.New("artifact Ref requires one JSON object")
	}
	seen := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return ref, err
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return ref, errors.New("duplicate or invalid artifact Ref field")
		}
		seen[name] = true
		switch name {
		case "id":
			err = d.Decode(&ref.ID)
		case "sha256":
			err = d.Decode(&ref.SHA256)
		case "size":
			// Proto JSON encodes int64 as a decimal string. Accept both the
			// canonical wire spelling and a JSON integer used in task YAML.
			var number json.Number
			err = d.Decode(&number)
			if err == nil {
				ref.Size, err = number.Int64()
				if err == nil && strconv.FormatInt(ref.Size, 10) != string(number) {
					err = errors.New("size requires a canonical decimal integer")
				}
			}
		default:
			return ref, fmt.Errorf("unknown artifact Ref field %q", name)
		}
		if err != nil {
			return ref, fmt.Errorf("decode artifact Ref %s: %w", name, err)
		}
	}
	if _, err := d.Token(); err != nil {
		return ref, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return ref, errors.New("artifact Ref requires exactly one JSON object")
	}
	return ref, ref.Validate()
}

func wireArtifactRef(ref artifact.Ref) *v1.ArtifactRef {
	return &v1.ArtifactRef{Id: ref.ID, Sha256: ref.SHA256, Size: ref.Size}
}

func checkedArtifactRef(ref *v1.ArtifactRef) (artifact.Ref, error) {
	if ref == nil {
		return artifact.Ref{}, errors.New("server returned no artifact reference")
	}
	r := artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}
	if err := r.Validate(); err != nil {
		return artifact.Ref{}, fmt.Errorf("server artifact reference: %w", err)
	}
	return r, nil
}

func workspaceTask(task *v1.Task) bool {
	return task != nil && (task.Workdir != nil || len(task.Setup) != 0 || len(task.Outputs) != 0)
}

func (o *options) submitWorkspace(ctx context.Context, client skyv1connect.ControlServiceClient, task *v1.Task, id string, domain *v1.RequestDomain) (*v1.Job, error) {
	r, err := client.SubmitWorkspaceJob(ctx, request(o, &v1.SubmitWorkspaceJobRequest{Task: task, RequestId: id, Domain: domain}))
	if err != nil {
		return nil, fmt.Errorf("submit workspace (retry the same task and request hints): %w", retentionDiagnostic(err))
	}
	if r == nil || r.Msg == nil || r.Msg.Job == nil {
		return nil, errors.New("server returned no job")
	}
	return r.Msg.Job, nil
}

// Only the new bounded artifact transport uses this receive limit. Existing
// task, job and discovery clients retain their established wire behavior.
func (o *options) artifactClient() skyv1connect.ControlServiceClient {
	return skyv1connect.NewControlServiceClient(http.DefaultClient, o.endpoint, connect.WithReadMaxBytes(128<<10))
}

// Ref JSON uses a numeric size so the exact object can also be pasted into a
// task's YAML workdir mapping. Status/job protobuf JSON retains its own format.
func writeArtifactRef(w io.Writer, ref artifact.Ref) error {
	return json.NewEncoder(w).Encode(ref)
}

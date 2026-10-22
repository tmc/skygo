package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	pageSize     = 50
	maxPageToken = 512
)

// validJobID reports whether id can name a job: 1..128 bytes of
// [A-Za-z0-9_-]. Other IDs are not found without an RPC.
func validJobID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func jobPath(id string) string { return "/jobs/" + url.PathEscape(id) }

func (s *server) rpcContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.timeout)
}

type jobRow struct{ ID, Link, Target, State string }

type jobsView struct {
	Jobs []jobRow
	Next string
}

func (s *server) jobs(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("page_token")
	if len(token) > maxPageToken || !utf8.ValidString(token) {
		s.message(w, http.StatusBadRequest, "invalid page token", "")
		return
	}
	ctx, cancel := s.rpcContext(r)
	defer cancel()
	resp, err := s.client.ListJobs(ctx, connect.NewRequest(&v1.ListJobsRequest{PageSize: pageSize, PageToken: token}))
	if err != nil {
		s.failure(w, err)
		return
	}
	var v jobsView
	for _, j := range resp.Msg.Jobs {
		v.Jobs = append(v.Jobs, jobRow{ID: j.Id, Link: jobPath(j.Id), Target: j.Target, State: state(j.State)})
	}
	if resp.Msg.NextPageToken != "" {
		v.Next = "/jobs?" + url.Values{"page_token": {resp.Msg.NextPageToken}}.Encode()
	}
	s.render(w, http.StatusOK, "jobs", "jobs", v)
}

type attemptRow struct {
	Ordinal                                   uint32
	ID, State, Reason, ExitCode, Result, Logs string
	Created, Starting, Finished               string
}

type jobView struct {
	ID, Target, State, Reason, ExitCode string
	Created, Updated, NextAttempt       string
	CancelRequested                     bool
	Result                              string
	Logs, Text                          string
	Attempts                            []attemptRow
	Argv, Setup, EnvKeys                []string
	Image, Resources, Workdir           string
	Retention, AcceptUntil              string
}

func (s *server) job(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validJobID(id) {
		s.notFound(w)
		return
	}
	ctx, cancel := s.rpcContext(r)
	defer cancel()
	resp, err := s.client.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: id}))
	if err != nil {
		s.failure(w, err)
		return
	}
	j := resp.Msg.Job
	if j == nil {
		s.message(w, http.StatusBadGateway, "request failed", "server returned no job")
		return
	}
	v := jobView{
		ID: j.Id, Target: j.Target, State: state(j.State), Reason: j.Reason, ExitCode: exitCode(j.ExitCode),
		Created: when(j.CreatedAt), Updated: when(j.UpdatedAt), NextAttempt: when(j.NextAttemptAt),
		CancelRequested: j.CancelRequested, Result: result(j.Result),
		Logs: jobPath(j.Id) + "/logs", Text: jobPath(j.Id) + "/logs.txt",
	}
	for _, a := range j.Attempts {
		logs := jobPath(j.Id) + "/logs"
		if a.Ordinal >= 1 && a.Ordinal <= 10 {
			logs += "?" + url.Values{"attempt": {strconv.FormatUint(uint64(a.Ordinal), 10)}}.Encode()
		}
		v.Attempts = append(v.Attempts, attemptRow{
			Ordinal: a.Ordinal, ID: a.Id, State: state(a.State), Reason: a.Reason, ExitCode: exitCode(a.ExitCode),
			Result: result(a.Result), Logs: logs, Created: when(a.CreatedAt), Starting: when(a.StartingAt), Finished: when(a.FinishedAt),
		})
	}
	if t := j.Task; t != nil {
		v.Argv, v.Setup, v.Image = t.Argv, t.Setup, t.Image
		v.Resources = resources(t)
		if t.Workdir != nil {
			v.Workdir = t.Workdir.Id
		}
		// Environment values may hold secrets; show names only.
		for k := range t.Env {
			v.EnvKeys = append(v.EnvKeys, k)
		}
		slices.Sort(v.EnvKeys)
	}
	v.Retention, v.AcceptUntil = s.retention(r)
	s.render(w, http.StatusOK, "job", "job "+j.Id, v)
}

// retention reports the server's retention status for the job page: enabled
// or disabled with its acceptance deadline, or why it is unknown. Error text
// from the server is not shown. A failure here does not fail the page.
func (s *server) retention(r *http.Request) (status, acceptUntil string) {
	ctx, cancel := s.rpcContext(r)
	defer cancel()
	resp, err := s.client.GetRetention(ctx, connect.NewRequest(&v1.GetRetentionRequest{}))
	switch {
	case connect.CodeOf(err) == connect.CodeUnimplemented:
		return "not supported by this server", ""
	case err != nil && connect.CodeOf(err) != connect.CodeUnknown:
		return "unavailable (" + connect.CodeOf(err).String() + ")", ""
	case err != nil:
		return "unavailable", ""
	case resp.Msg == nil:
		return "unavailable (no response)", ""
	case !resp.Msg.Enabled:
		return "disabled", ""
	}
	return "enabled", when(resp.Msg.AcceptUntil)
}

func resources(t *v1.Task) string {
	var parts []string
	if t.CpuMillis != 0 {
		parts = append(parts, fmt.Sprintf("cpu=%dm", t.CpuMillis))
	}
	if t.MemoryBytes != 0 {
		parts = append(parts, fmt.Sprintf("memory=%d", t.MemoryBytes))
	}
	if t.CpuLimitMillis != 0 {
		parts = append(parts, fmt.Sprintf("cpu-limit=%dm", t.CpuLimitMillis))
	}
	if t.MemoryLimitBytes != 0 {
		parts = append(parts, fmt.Sprintf("memory-limit=%d", t.MemoryLimitBytes))
	}
	if t.Accelerator != "" {
		parts = append(parts, fmt.Sprintf("accelerator=%s:%d", t.Accelerator, t.AcceleratorCount))
	}
	return strings.Join(parts, " ")
}

type targetRow struct {
	Name, Kind           string
	Labels, Accelerators []string
	Capabilities         []string
}

func targetRows(targets []*v1.Target) []targetRow {
	var rows []targetRow
	for _, t := range targets {
		row := targetRow{Name: t.Name, Kind: t.Kind, Accelerators: t.AcceleratorNames}
		for k, v := range t.Labels {
			row.Labels = append(row.Labels, k+"="+v)
		}
		slices.Sort(row.Labels)
		for _, c := range t.Capabilities {
			row.Capabilities = append(row.Capabilities, capabilityName(c))
		}
		rows = append(rows, row)
	}
	return rows
}

func (s *server) targets(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.rpcContext(r)
	defer cancel()
	resp, err := s.client.ListTargets(ctx, connect.NewRequest(&v1.ListTargetsRequest{}))
	if err != nil {
		s.failure(w, err)
		return
	}
	s.render(w, http.StatusOK, "targets", "targets", targetRows(resp.Msg.Targets))
}

type infoView struct {
	Endpoint, Absent []string
	Targets          []targetRow
}

// endpointCapabilities are installed endpoint services, as opposed to
// per-target execution semantics.
var endpointCapabilities = []v1.Capability{
	v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1,
	v1.Capability_CAPABILITY_REMOTE_DATA_TRANSFER_V1,
}

func (s *server) info(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.rpcContext(r)
	defer cancel()
	resp, err := s.client.GetServerInfo(ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
	if err != nil {
		s.failure(w, err)
		return
	}
	v := infoView{Targets: targetRows(resp.Msg.Targets)}
	for _, c := range resp.Msg.Capabilities {
		v.Endpoint = append(v.Endpoint, capabilityName(c))
	}
	for _, c := range endpointCapabilities {
		if !slices.Contains(resp.Msg.Capabilities, c) {
			v.Absent = append(v.Absent, capabilityName(c))
		}
	}
	s.render(w, http.StatusOK, "info", "server information", v)
}

// capabilityName matches the CLI's names: "finite-jobs-v1", or "unknown(N)".
func capabilityName(c v1.Capability) string {
	name, ok := v1.Capability_name[int32(c)]
	if !ok || c == v1.Capability_CAPABILITY_UNSPECIFIED {
		return fmt.Sprintf("unknown(%d)", c)
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, "CAPABILITY_")), "_", "-")
}

func state(s v1.JobState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATE_"))
}

func exitCode(c *int32) string {
	if c == nil {
		return ""
	}
	return strconv.Itoa(int(*c))
}

func when(t *timestamppb.Timestamp) string {
	if t == nil {
		return ""
	}
	return t.AsTime().UTC().Format(time.RFC3339)
}

// result shows an artifact result's state and attempt, never a download.
func result(r *v1.ArtifactResult) string {
	if r == nil {
		return ""
	}
	s := strings.ToLower(strings.TrimPrefix(r.State.String(), "ARTIFACT_RESULT_STATE_"))
	if r.AttemptId != "" {
		s += " (attempt " + r.AttemptId + ")"
	}
	return s
}

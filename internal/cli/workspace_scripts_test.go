package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// The refusal scripts execute the real CLI in child processes. This endpoint
// counts every request, including discovery, rather than accepting a stubbed
// Begin/Submit as evidence that validation preceded all network operations.
func TestWorkspaceRefusalScripts(t *testing.T) {
	for _, name := range []string{"validation", "path_refusals", "resume_refusals"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "unexpected workspace request", http.StatusServiceUnavailable)
			}))
			t.Cleanup(srv.Close)
			e := scriptEngine(t)
			workspaceScriptCommands(e, &calls)
			scripttest.Test(t, context.Background(), e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=workspace-token"}, "testdata/workspaces/"+name+".txt")
		})
	}
}

func workspaceScriptCommands(e *script.Engine, calls *atomic.Int64) {
	e.Cmds["workspace-canonical"] = script.Command(script.CmdUsage{Summary: "use the actual nonsymlink test directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		path, err := filepath.EvalSymlinks(s.Getwd())
		if err != nil {
			return nil, err
		}
		if err := s.Chdir(path); err != nil {
			return nil, err
		}
		return nil, s.Setenv("WORK", path)
	})
	e.Cmds["workspace-private"] = script.Command(script.CmdUsage{Summary: "make a private test fixture directory and its files", Args: "DIR"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		path := s.Path(args[0])
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
		return nil, filepath.WalkDir(path, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("private fixture cannot contain symlinks")
			}
			mode := os.FileMode(0600)
			if entry.IsDir() {
				mode = 0700
			}
			return os.Chmod(path, mode)
		})
	})
	e.Cmds["workspace-requests"] = script.Command(script.CmdUsage{Summary: "check all actual endpoint requests", Args: "COUNT"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return nil, err
		}
		if got := calls.Load(); got != want {
			return nil, fmt.Errorf("endpoint requests=%d want=%d", got, want)
		}
		return nil, nil
	})
}

// This negative-only advertisement fixture is not an old daemon or a modeled
// successful workspace service. It can only answer discovery and must never
// receive an artifact/job write or cause a downgrade to the legacy Begin.
type workspaceOldInfoService struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (*workspaceOldInfoService) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{Capabilities: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}), nil
}

func TestWorkspaceCapabilityScript(t *testing.T) {
	var calls atomic.Int64
	var writes atomic.Int64
	path, handler := skyv1connect.NewControlServiceHandler(new(workspaceOldInfoService))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if filepath.Base(r.URL.Path) != "GetServerInfo" {
			writes.Add(1)
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	e := scriptEngine(t)
	workspaceScriptCommands(e, &calls)
	t.Cleanup(func() {
		if got := writes.Load(); got != 0 {
			t.Errorf("missing capability made %d non-discovery requests", got)
		}
	})
	scripttest.Test(t, context.Background(), e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=workspace-token"}, "testdata/workspaces/capability.txt")
}

package kube

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
)

// Field order and empty objects are part of the binding format.
type kubeBindingV1 struct {
	Format        string            `json:"format"`
	Kind          string            `json:"kind"`
	Server        string            `json:"server"`
	Namespace     string            `json:"namespace"`
	Labels        map[string]string `json:"labels"`
	TLSServerName string            `json:"tls_server_name"`
	CASHA256      string            `json:"ca_sha256"`
	Insecure      bool              `json:"insecure_skip_verify"`
	Policy        bindingPolicyV1   `json:"policy"`
}

type bindingPolicyV1 struct {
	MaxCPUMillis    int64                           `json:"max_cpu_millis"`
	MaxMemoryBytes  int64                           `json:"max_memory_bytes"`
	MaxAccelerators int32                           `json:"max_accelerators"`
	Accelerators    map[string]bindingAcceleratorV1 `json:"accelerators"`
	NodeSelectors   map[string][]string             `json:"node_selectors"`
}

type bindingAcceleratorV1 struct {
	ResourceName         string            `json:"resource_name"`
	RequiredNodeSelector map[string]string `json:"required_node_selector"`
}

// kubeDataBindingV1 is the binding of a target with a data profile. The
// embedded legacy fields encode first, in their legacy order.
type kubeDataBindingV1 struct {
	kubeBindingV1
	Data bindingDataV1 `json:"data"`
}

type bindingDataV1 struct {
	Layout                 string                   `json:"layout"`
	ProfileVersion         string                   `json:"profile_version"`
	DataPolicy             bindingDataPolicyV1      `json:"data_policy"`
	UID                    int64                    `json:"uid"`
	GID                    int64                    `json:"gid"`
	FSGroup                int64                    `json:"fs_group"`
	Seccomp                string                   `json:"seccomp"`
	CapabilitiesDrop       []string                 `json:"capabilities_drop"`
	ReadOnlyRootFilesystem bool                     `json:"read_only_root_filesystem"`
	AutomountToken         bool                     `json:"automount_service_account_token"`
	Sidecars               bool                     `json:"sidecars"`
	OwnerReferences        bool                     `json:"owner_references"`
	HelperImage            string                   `json:"helper_image"`
	HelperSHA256           string                   `json:"helper_sha256"`
	TaskImages             []string                 `json:"task_images"`
	Qualification          string                   `json:"qualification"`
	Origin                 string                   `json:"origin"`
	Host                   string                   `json:"host"`
	CASHA256               string                   `json:"data_ca_sha256"`
	TLSMin                 string                   `json:"tls_min"`
	ServiceRoute           string                   `json:"service_route"`
	ServiceMethods         []string                 `json:"service_methods"`
	Redirects              string                   `json:"redirects"`
	Proxy                  string                   `json:"proxy"`
	ProtectedPaths         []string                 `json:"protected_paths"`
	LayoutPaths            bindingLayoutPathsV1     `json:"layout_paths"`
	References             []bindingDataReferenceV1 `json:"references"`
	ReplacementPolicy      string                   `json:"replacement_policy"`
}

type bindingDataPolicyV1 struct {
	StageSeconds       int64 `json:"stage_seconds"`
	CollectionSeconds  int64 `json:"collection_seconds"`
	DefaultTaskSeconds int64 `json:"default_task_seconds"`
	MaxTaskSeconds     int64 `json:"max_task_seconds"`
	ShutdownSeconds    int64 `json:"shutdown_seconds"`
}

type bindingLayoutPathsV1 struct {
	Workspace     string `json:"workspace"`
	Stage         string `json:"stage"`
	Work          string `json:"work"`
	HelperMount   string `json:"helper_mount"`
	HelperCopy    string `json:"helper_copy"`
	HelperEntry   string `json:"helper_entry"`
	InputTokenDir string `json:"input_token_dir"`
	OutputToken   string `json:"output_token_dir"`
}

type bindingDataReferenceV1 struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	Resource     string   `json:"resource"`
	Keys         []string `json:"keys"`
	Destinations []string `json:"destinations"`
	AllowWrite   bool     `json:"allow_write"`
}

// kubeDRABindingV1 is the binding of a target with at least one DRA
// accelerator. The embedded legacy fields encode first, in their legacy
// order; a DRA accelerator's legacy entry has an empty resource name.
type kubeDRABindingV1 struct {
	kubeBindingV1
	DRA map[string]bindingDRAV1 `json:"dra"`
}

// bindingDRAV1 binds one DRA accelerator: its operator class and selectors,
// in order, and the fixed claim shape of this mode (dra_identity.go).
type bindingDRAV1 struct {
	DeviceClass    string   `json:"device_class"`
	Selectors      []string `json:"selectors"`
	AllocationMode string   `json:"allocation_mode"`
	Request        string   `json:"request"`
	Reference      string   `json:"reference"`
}

func bindingDRA(p ResourcePolicy) map[string]bindingDRAV1 {
	m := make(map[string]bindingDRAV1)
	for name, a := range p.Accelerators {
		if a.DRA == nil {
			continue
		}
		selectors := slices.Clone(a.DRA.Selectors)
		if selectors == nil {
			selectors = []string{}
		}
		m[name] = bindingDRAV1{a.DRA.DeviceClass, selectors, draAllocationMode, draRequestName, draReference}
	}
	return m
}

// targetBinding returns the canonical identity of a target. With d nil and no
// DRA accelerator it is exactly the legacy format and q is ignored; with a
// DRA accelerator it is the DRA format; otherwise it is the data format,
// which also binds d and the qualification identity of q, if any.
func targetBinding(t TargetConfig, config *rest.Config, d *dataProfile, q *dataQualification) (string, error) {
	raw, err := bindingJSON(t, config, d, q)
	if err != nil {
		return "", errors.New("encode kubernetes target binding")
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), nil
}

// bindingJSON returns the canonical encoding hashed by targetBinding.
func bindingJSON(t TargetConfig, config *rest.Config, d *dataProfile, q *dataQualification) ([]byte, error) {
	ca := "system-roots"
	if len(config.CAData) != 0 {
		ca = fmt.Sprintf("%x", sha256.Sum256(config.CAData))
	}
	labels := t.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	p := clonePolicy(t.Resources)
	selectors := p.NodeSelectors
	if selectors == nil {
		selectors = map[string][]string{}
	}
	accelerators := make(map[string]bindingAcceleratorV1, len(p.Accelerators))
	for name, a := range p.Accelerators {
		required := a.RequiredNodeSelector
		if required == nil {
			required = map[string]string{}
		}
		accelerators[name] = bindingAcceleratorV1{a.ResourceName, required}
	}
	v := kubeBindingV1{
		Format: "skygo-kubernetes-target-v1", Kind: "kubernetes", Server: config.Host,
		Namespace: t.Namespace, Labels: labels, TLSServerName: config.ServerName,
		CASHA256: ca, Insecure: config.Insecure,
		Policy: bindingPolicyV1{p.MaxCPUMillis, p.MaxMemoryBytes, p.MaxAccelerators, accelerators, selectors},
	}
	if p.hasDRA() {
		// validateTarget refuses DRA with a data block.
		if d != nil {
			return nil, errors.New("dra accelerators conflict with a data block")
		}
		v.Format = "skygo-kubernetes-dra-target-v1"
		return json.Marshal(kubeDRABindingV1{v, bindingDRA(p)})
	}
	if d == nil {
		return json.Marshal(v)
	}
	v.Format = "skygo-kubernetes-data-target-v1"
	return json.Marshal(kubeDataBindingV1{v, bindingData(d, q)})
}

func bindingData(d *dataProfile, q *dataQualification) bindingDataV1 {
	p := backend.DefaultDataPolicy()
	qualification := ""
	if q != nil {
		qualification = q.id
	}
	names := slices.Sorted(maps.Keys(d.references))
	refs := make([]bindingDataReferenceV1, 0, len(names))
	for _, name := range names {
		r := d.references[name]
		kind, resource := "pvc", r.pvc
		if r.secret != "" {
			kind, resource = "secret", r.secret
		}
		keys := slices.Sorted(slices.Values(r.keys))
		if keys == nil {
			keys = []string{}
		}
		destinations := slices.Sorted(slices.Values(r.destinations))
		if destinations == nil {
			destinations = []string{}
		}
		refs = append(refs, bindingDataReferenceV1{name, kind, resource, keys, destinations, r.allowWrite})
	}
	images := slices.Sorted(slices.Values(d.taskImages))
	if images == nil {
		images = []string{}
	}
	return bindingDataV1{
		Layout: dataLayout, ProfileVersion: dataProfileVersion,
		DataPolicy: bindingDataPolicyV1{p.StageSeconds, p.CollectionSeconds,
			p.DefaultTaskSeconds, p.MaxTaskSeconds, p.ShutdownSeconds},
		UID: dataUser, GID: dataUser, FSGroup: dataUser,
		Seccomp: "RuntimeDefault", CapabilitiesDrop: []string{"ALL"},
		ReadOnlyRootFilesystem: true,
		HelperImage:            d.helperImage, HelperSHA256: d.helperSHA256,
		TaskImages: images, Qualification: qualification,
		Origin: d.origin, Host: d.host, CASHA256: d.caSHA256,
		TLSMin: "1.2", ServiceRoute: dataServiceRoute, ServiceMethods: dataServiceMethods(), Redirects: "refuse", Proxy: "none",
		ProtectedPaths: protectedPaths(),
		LayoutPaths: bindingLayoutPathsV1{workspaceMount, stagePath, workPath,
			helperMount, helperCopy, helperEntry, inputTokenDir, outputTokenDir},
		References: refs, ReplacementPolicy: dataReplacementPolicy,
	}
}

func canonicalServer(server string) (string, error) {
	invalid := errors.New("invalid kubernetes API server URL")
	u, err := url.Parse(server)
	if err != nil || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(server, "#") {
		return "", invalid
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", invalid
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.Contains(host, "%") {
		return "", invalid
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	} else if len(validation.IsDNS1123Subdomain(strings.TrimSuffix(host, "."))) != 0 {
		return "", invalid
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", invalid
		}
		port = strconv.Itoa(n)
		if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
			port = ""
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return "", invalid
	}
	for _, part := range strings.Split(u.Path, "/") {
		if part == "." || part == ".." {
			return "", invalid
		}
	}
	// client-go joins API paths using the decoded URL path. Refuse encodings
	// whose distinction would be lost, such as an escaped slash or semicolon.
	plain := (&url.URL{Path: u.Path}).EscapedPath()
	if strings.ContainsAny(u.Path, "\x00\r\n") || strings.Contains(u.Path, "//") || u.EscapedPath() != plain {
		return "", invalid
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	return u.String(), nil
}

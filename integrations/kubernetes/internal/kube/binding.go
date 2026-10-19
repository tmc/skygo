package kube

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

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

func targetBinding(t TargetConfig, config *rest.Config) (string, error) {
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
	raw, err := json.Marshal(v)
	if err != nil {
		return "", errors.New("encode kubernetes target binding")
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), nil
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

package kube

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	profileID    = "0123456789abcdef0123456789abcdef"
	profileHash  = "feedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface"
	profileImage = "registry.example/task@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	profileHelp  = "registry.example/helper@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	profileCA    = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
	profileToken = "s3cr3t-profile-token-bytes"
)

// profileBackend returns a Backend with a labeled test data profile. It is a
// fixture, not a qualified production profile.
func profileBackend() *Backend {
	return &Backend{
		namespace: "jobs", owner: "owner", binding: "sha256:binding",
		policy: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{
			"nvidia": {ResourceName: "nvidia.com/gpu"}}},
		data: &dataProfile{
			helperImage:  profileHelp,
			helperSHA256: "3333333333333333333333333333333333333333333333333333333333333333",
			origin:       "https://data.example",
			host:         "data.example",
			caPEM:        []byte(profileCA),
			caSHA256:     "4444444444444444444444444444444444444444444444444444444444444444",
			taskImages:   []string{profileImage},
			references: map[string]dataReference{
				"models": {pvc: "models-pvc", destinations: []string{"/mnt/skygo/models"}, allowWrite: true},
				"cache":  {pvc: "cache-pvc", destinations: []string{"/mnt/skygo/cache"}},
				"creds":  {secret: "creds-secret", keys: []string{"a", "b"}, destinations: []string{"/mnt/skygo/creds"}},
			},
		},
	}
}

func profileTask() backend.Task {
	return backend.Task{
		Argv: []string{"python", "train.py"}, Image: profileImage,
		Env:       map[string]string{"B": "2", "A": "1"},
		CPUMillis: 500, MemoryBytes: 1 << 30, Accelerator: "nvidia", AcceleratorCount: 1,
		NodeSelector: map[string]string{"pool": "gpu"}, TimeoutSeconds: 60,
	}
}

func profileWorkdir() *artifact.Ref {
	return &artifact.Ref{ID: "abcdefabcdefabcdefabcdefabcdefab",
		SHA256: "5555555555555555555555555555555555555555555555555555555555555555", Size: 1234}
}

func profileBudgets(t *testing.T) backend.DataBudgets {
	t.Helper()
	b, err := backend.DefaultDataPolicy().Budgets(60)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// profileMounts formats mounts as "volume:path:ro" or "volume:path:rw".
func profileMounts(mounts []corev1.VolumeMount) []string {
	var s []string
	for _, m := range mounts {
		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		s = append(s, m.Name+":"+m.MountPath+":"+mode)
	}
	return s
}

func profileContainers(p *corev1.Pod) []corev1.Container {
	return append(slices.Clone(p.Spec.InitContainers), p.Spec.Containers...)
}

func TestDataPodProfiles(t *testing.T) {
	const (
		ws     = "workspace:/workspace:rw"
		wsRO   = "workspace:/workspace:ro"
		helpRW = "helper:/opt/skygo:rw"
		helpRO = "helper:/opt/skygo:ro"
		in     = "input-token:/run/skygo/input:ro"
		out    = "output-token:/run/skygo/output:ro"
	)
	stage := []string{ws, helpRW, in}
	workload := []string{ws, helpRO}
	collector := []string{wsRO, out}
	tests := []struct {
		name       string
		edit       func(*backend.Task)
		inits      []string
		mains      []string
		volumes    []string
		mounts     map[string][]string
		commands   map[string][]string
		workingDir map[string]string
	}{
		{
			name:    "workdir",
			edit:    func(t *backend.Task) { t.Workdir = profileWorkdir() },
			inits:   []string{"stage"},
			mains:   []string{"task"},
			volumes: []string{"workspace", "helper", "input-token"},
			mounts:  map[string][]string{"stage": stage, "task": workload},
			commands: map[string][]string{
				"stage": {"/skygo-data", "stage"},
				"task":  {"/opt/skygo/bin/skygo-data", "exec", "--", "python", "train.py"},
			},
			workingDir: map[string]string{"stage": "", "task": "/workspace/work"},
		},
		{
			name: "workdir setup",
			edit: func(t *backend.Task) {
				t.Workdir = profileWorkdir()
				t.Setup = []string{"pip", "install", "."}
			},
			inits:   []string{"stage", "setup"},
			mains:   []string{"task"},
			volumes: []string{"workspace", "helper", "input-token"},
			mounts:  map[string][]string{"stage": stage, "setup": workload, "task": workload},
			commands: map[string][]string{
				"setup": {"/opt/skygo/bin/skygo-data", "exec", "--", "pip", "install", "."},
			},
			workingDir: map[string]string{"setup": "/workspace/work"},
		},
		{
			name: "workdir outputs",
			edit: func(t *backend.Task) {
				t.Workdir = profileWorkdir()
				t.Outputs = []string{"out", "logs"}
			},
			inits:   []string{"stage"},
			mains:   []string{"task", "collector"},
			volumes: []string{"workspace", "helper", "input-token", "output-token"},
			mounts:  map[string][]string{"stage": stage, "task": workload, "collector": collector},
			commands: map[string][]string{
				"collector": {"/skygo-data", "collect", "--", "out", "logs"},
			},
			workingDir: map[string]string{"collector": "/workspace/work"},
		},
		{
			name: "workdir setup outputs aliases",
			edit: func(t *backend.Task) {
				t.Workdir = profileWorkdir()
				t.Setup = []string{"make"}
				t.Outputs = []string{"out"}
				t.DataMounts = []backend.DataMount{
					{Reference: "models", Destination: "/mnt/skygo/models", ReadOnly: true},
					{Reference: "creds", Destination: "/mnt/skygo/creds", ReadOnly: true},
				}
			},
			inits:   []string{"stage", "setup"},
			mains:   []string{"task", "collector"},
			volumes: []string{"workspace", "helper", "input-token", "output-token", "alias-0", "alias-1"},
			mounts: map[string][]string{
				"stage":     stage,
				"setup":     {ws, helpRO, "alias-0:/mnt/skygo/models:ro", "alias-1:/mnt/skygo/creds:ro"},
				"task":      {ws, helpRO, "alias-0:/mnt/skygo/models:ro", "alias-1:/mnt/skygo/creds:ro"},
				"collector": collector,
			},
		},
		{
			name: "mount-only pvc rw and secret",
			edit: func(t *backend.Task) {
				t.DataMounts = []backend.DataMount{
					{Reference: "models", Destination: "/mnt/skygo/models"},
					{Reference: "creds", Destination: "/mnt/skygo/creds"},
				}
			},
			mains:      []string{"task"},
			volumes:    []string{"alias-0", "alias-1"},
			mounts:     map[string][]string{"task": {"alias-0:/mnt/skygo/models:rw", "alias-1:/mnt/skygo/creds:ro"}},
			commands:   map[string][]string{"task": {"python", "train.py"}},
			workingDir: map[string]string{"task": ""},
		},
		{
			name: "mount-only directory",
			edit: func(t *backend.Task) {
				t.Directory = "/srv/app"
				t.DataMounts = []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache", ReadOnly: true}}
			},
			mains:      []string{"task"},
			volumes:    []string{"alias-0"},
			mounts:     map[string][]string{"task": {"alias-0:/mnt/skygo/cache:ro"}},
			commands:   map[string][]string{"task": {"python", "train.py"}},
			workingDir: map[string]string{"task": "/srv/app"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := profileBackend()
			task := profileTask()
			tt.edit(&task)
			p, err := b.dataPod(profileID, task, profileBudgets(t), profileHash)
			if err != nil {
				t.Fatal(err)
			}
			again, err := b.dataPod(profileID, task, profileBudgets(t), profileHash)
			if err != nil || !reflect.DeepEqual(p, again) {
				t.Fatalf("dataPod is not deterministic: %v", err)
			}
			if err := b.dataPodMatches(p, again); err != nil {
				t.Fatalf("dataPodMatches(built, rebuilt) = %v", err)
			}

			if p.Name != "skygo-"+profileID || p.Namespace != "jobs" ||
				!reflect.DeepEqual(p.Labels, map[string]string{ownerLabel: "owner", idLabel: profileID}) ||
				!reflect.DeepEqual(p.Annotations, map[string]string{taskAnnotation: profileHash,
					bindingAnnotation: "sha256:binding", layoutAnnotation: dataLayout}) {
				t.Fatalf("metadata = %+v", p.ObjectMeta)
			}
			var inits, mains, volumes []string
			for _, c := range p.Spec.InitContainers {
				inits = append(inits, c.Name)
			}
			for _, c := range p.Spec.Containers {
				mains = append(mains, c.Name)
			}
			for _, v := range p.Spec.Volumes {
				volumes = append(volumes, v.Name)
			}
			if !slices.Equal(inits, tt.inits) || !slices.Equal(mains, tt.mains) {
				t.Fatalf("containers = %v %v, want %v %v", inits, mains, tt.inits, tt.mains)
			}
			if !slices.Equal(volumes, tt.volumes) {
				t.Fatalf("volumes = %v, want %v", volumes, tt.volumes)
			}

			s := p.Spec
			if s.RestartPolicy != corev1.RestartPolicyNever || s.ActiveDeadlineSeconds == nil ||
				*s.ActiveDeadlineSeconds != 600+60+600+120 ||
				s.AutomountServiceAccountToken == nil || *s.AutomountServiceAccountToken ||
				s.EnableServiceLinks == nil || *s.EnableServiceLinks ||
				s.ShareProcessNamespace == nil || *s.ShareProcessNamespace ||
				s.HostNetwork || s.HostPID || s.HostIPC || len(s.EphemeralContainers) != 0 ||
				!reflect.DeepEqual(s.NodeSelector, map[string]string{"pool": "gpu"}) {
				t.Fatalf("pod spec = %+v", s)
			}
			psc := s.SecurityContext
			if psc == nil || !*psc.RunAsNonRoot || *psc.RunAsUser != dataUser || *psc.RunAsGroup != dataUser ||
				*psc.FSGroup != dataUser || *psc.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch ||
				psc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
				t.Fatalf("pod security context = %+v", psc)
			}

			for _, c := range profileContainers(p) {
				if got := profileMounts(c.VolumeMounts); !slices.Equal(got, tt.mounts[c.Name]) {
					t.Errorf("%s mounts = %v, want %v", c.Name, got, tt.mounts[c.Name])
				}
				if want, ok := tt.commands[c.Name]; ok && !slices.Equal(c.Command, want) {
					t.Errorf("%s command = %q, want %q", c.Name, c.Command, want)
				}
				if want, ok := tt.workingDir[c.Name]; ok && c.WorkingDir != want {
					t.Errorf("%s working directory = %q, want %q", c.Name, c.WorkingDir, want)
				}
				if len(c.Args) != 0 || len(c.EnvFrom) != 0 || len(c.Ports) != 0 || c.RestartPolicy != nil {
					t.Errorf("%s has unexpected fields", c.Name)
				}
				sc := c.SecurityContext
				if sc == nil || !*sc.RunAsNonRoot || *sc.RunAsUser != dataUser || *sc.RunAsGroup != dataUser ||
					*sc.AllowPrivilegeEscalation || !*sc.ReadOnlyRootFilesystem || sc.Privileged != nil ||
					!slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) || len(sc.Capabilities.Add) != 0 ||
					sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
					t.Errorf("%s security context = %+v", c.Name, sc)
				}
				var env []string
				for _, e := range c.Env {
					env = append(env, e.Name)
				}
				switch c.Name {
				case "stage":
					want := []string{"SKYGO_DATA_CA_PEM", "SKYGO_DATA_HELPER_SHA256", "SKYGO_DATA_INPUT_ID",
						"SKYGO_DATA_INPUT_SHA256", "SKYGO_DATA_INPUT_SIZE", "SKYGO_DATA_ORIGIN"}
					if !slices.Equal(env, want) || c.Env[0].Value != profileCA || c.Env[4].Value != "1234" ||
						c.Env[2].Value != task.Workdir.ID || c.Env[3].Value != task.Workdir.SHA256 {
						t.Errorf("stage env = %+v", c.Env)
					}
				case "collector":
					if !slices.Equal(env, []string{"SKYGO_DATA_CA_PEM", "SKYGO_DATA_ORIGIN"}) {
						t.Errorf("collector env = %v", env)
					}
				default:
					if !slices.Equal(env, []string{"A", "B"}) {
						t.Errorf("%s env = %v", c.Name, env)
					}
				}
				switch c.Name {
				case "stage", "collector":
					if c.Image != profileHelp || len(c.Resources.Requests) != 0 || len(c.Resources.Limits) != 0 {
						t.Errorf("%s image or resources = %q %+v", c.Name, c.Image, c.Resources)
					}
				default:
					cpu, gpu := c.Resources.Requests[corev1.ResourceCPU], c.Resources.Limits["nvidia.com/gpu"]
					if c.Image != profileImage || cpu.Cmp(resource.MustParse("500m")) != 0 || gpu.Value() != 1 {
						t.Errorf("%s image or resources = %q %+v", c.Name, c.Image, c.Resources)
					}
				}
			}
			for _, v := range p.Spec.Volumes {
				if v.HostPath != nil || v.Projected != nil {
					t.Errorf("volume %s has a forbidden source", v.Name)
				}
				if v.Secret != nil && (v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o440 || len(v.Secret.Items) == 0) {
					t.Errorf("secret volume %s = %+v", v.Name, v.Secret)
				}
			}
		})
	}
}

func TestDataPodAliasSources(t *testing.T) {
	b := profileBackend()
	task := profileTask()
	task.DataMounts = []backend.DataMount{
		{Reference: "models", Destination: "/mnt/skygo/models"},
		{Reference: "creds", Destination: "/mnt/skygo/creds"},
	}
	p, err := b.dataPod(profileID, task, profileBudgets(t), profileHash)
	if err != nil {
		t.Fatal(err)
	}
	pvc := p.Spec.Volumes[0].PersistentVolumeClaim
	if pvc == nil || pvc.ClaimName != "models-pvc" || pvc.ReadOnly {
		t.Errorf("pvc alias = %+v", pvc)
	}
	sec := p.Spec.Volumes[1].Secret
	if sec == nil || sec.SecretName != "creds-secret" || sec.Optional == nil || *sec.Optional ||
		!reflect.DeepEqual(sec.Items, []corev1.KeyToPath{{Key: "a", Path: "a"}, {Key: "b", Path: "b"}}) {
		t.Errorf("secret alias = %+v", sec)
	}
}

func TestDataPodErrors(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*Backend, *backend.Task)
		budgets backend.DataBudgets
	}{
		{"no profile", func(b *Backend, _ *backend.Task) { b.data = nil }, backend.DataBudgets{}},
		{"unknown reference", func(_ *Backend, t *backend.Task) {
			t.DataMounts = []backend.DataMount{{Reference: "missing", Destination: "/mnt/skygo/x", ReadOnly: true}}
		}, backend.DataBudgets{}},
		{"write to read-only reference", func(_ *Backend, t *backend.Task) {
			t.DataMounts = []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}
		}, backend.DataBudgets{}},
		{"invalid reference", func(b *Backend, t *backend.Task) {
			b.data.references["bad"] = dataReference{pvc: "p", secret: "s"}
			t.DataMounts = []backend.DataMount{{Reference: "bad", Destination: "/mnt/skygo/bad", ReadOnly: true}}
		}, backend.DataBudgets{}},
		{"invalid budgets", func(*Backend, *backend.Task) {}, backend.DataBudgets{StageSeconds: 1}},
		{"setup without workdir", func(_ *Backend, t *backend.Task) { t.Setup = []string{"make"} }, backend.DataBudgets{}},
		{"outputs without workdir", func(_ *Backend, t *backend.Task) { t.Outputs = []string{"out"} }, backend.DataBudgets{}},
		{"workdir with directory", func(_ *Backend, t *backend.Task) {
			t.Workdir = profileWorkdir()
			t.Directory = "/srv"
		}, backend.DataBudgets{}},
		{"no argv", func(_ *Backend, t *backend.Task) { t.Argv = nil }, backend.DataBudgets{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := profileBackend()
			task := profileTask()
			tt.edit(b, &task)
			budgets := tt.budgets
			if budgets == (backend.DataBudgets{}) {
				budgets = profileBudgets(t)
			}
			if p, err := b.dataPod(profileID, task, budgets, profileHash); err == nil || p != nil {
				t.Fatalf("dataPod = %v, %v; want error", p, err)
			}
		})
	}
	if _, err := profileBackend().dataPod("bad", profileTask(), profileBudgets(t), profileHash); err == nil {
		t.Fatal("dataPod accepted an invalid id")
	}
}

// profileServerDefaults simulates fields an API server and admission add.
func profileServerDefaults(p *corev1.Pod) {
	grace := int64(30)
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	p.UID = "uid"
	p.Annotations["kubernetes.io/psp"] = "restricted"
	s := &p.Spec
	s.DNSPolicy = corev1.DNSClusterFirst
	s.SchedulerName = corev1.DefaultSchedulerName
	s.ServiceAccountName = "default"
	s.DeprecatedServiceAccount = "default"
	s.NodeName = "node-1"
	s.Priority = &priority
	s.PreemptionPolicy = &preempt
	s.TerminationGracePeriodSeconds = &grace
	s.Tolerations = []corev1.Toleration{{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists,
		Effect: corev1.TaintEffectNoExecute}}
	for _, cs := range [][]corev1.Container{s.InitContainers, s.Containers} {
		for i := range cs {
			cs[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
			cs[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			cs[i].ImagePullPolicy = corev1.PullIfNotPresent
		}
	}
	p.Status.Phase = corev1.PodRunning
}

func TestDataPodMatches(t *testing.T) {
	b := profileBackend()
	task := profileTask()
	task.Workdir = profileWorkdir()
	task.Setup = []string{"make"}
	task.Outputs = []string{"out"}
	task.DataMounts = []backend.DataMount{{Reference: "models", Destination: "/mnt/skygo/models", ReadOnly: true}}
	want, err := b.dataPod(profileID, task, profileBudgets(t), profileHash)
	if err != nil {
		t.Fatal(err)
	}
	observed := func() *corev1.Pod {
		p := want.DeepCopy()
		profileServerDefaults(p)
		return p
	}
	if err := b.dataPodMatches(want, observed()); err != nil {
		t.Fatalf("dataPodMatches(defaulted) = %v", err)
	}
	nilShare := observed()
	nilShare.Spec.ShareProcessNamespace = nil
	if err := b.dataPodMatches(want, nilShare); err != nil {
		t.Fatalf("dataPodMatches(nil ShareProcessNamespace) = %v", err)
	}
	yes := true
	zero := int64(0)
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"name", func(p *corev1.Pod) { p.Name += "x" }},
		{"namespace", func(p *corev1.Pod) { p.Namespace = "other" }},
		{"extra label", func(p *corev1.Pod) { p.Labels["extra"] = "x" }},
		{"missing label", func(p *corev1.Pod) { delete(p.Labels, idLabel) }},
		{"task annotation", func(p *corev1.Pod) { p.Annotations[taskAnnotation] = "other" }},
		{"missing layout annotation", func(p *corev1.Pod) { delete(p.Annotations, layoutAnnotation) }},
		{"skygo annotation", func(p *corev1.Pod) { p.Annotations["skygo.tmc.dev/extra"] = "x" }},
		{"owner reference", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: "x", UID: "u"}}
		}},
		{"extra container", func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "proxy", Image: "proxy"})
		}},
		{"injected sidecar init", func(p *corev1.Pod) {
			always := corev1.ContainerRestartPolicyAlways
			p.Spec.InitContainers = append(p.Spec.InitContainers, corev1.Container{Name: "mesh", Image: "mesh", RestartPolicy: &always})
		}},
		{"init restart policy", func(p *corev1.Pod) {
			always := corev1.ContainerRestartPolicyAlways
			p.Spec.InitContainers[1].RestartPolicy = &always
		}},
		{"ephemeral container", func(p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "busybox"}}}
		}},
		{"container order", func(p *corev1.Pod) {
			c := p.Spec.Containers
			c[0], c[1] = c[1], c[0]
		}},
		{"image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = "busybox" }},
		{"helper image", func(p *corev1.Pod) { p.Spec.InitContainers[0].Image = "busybox" }},
		{"command", func(p *corev1.Pod) { p.Spec.Containers[0].Command[3] = "sh" }},
		{"args", func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"-c"} }},
		{"env", func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = "changed" }},
		{"env from", func(p *corev1.Pod) {
			p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}}}}
		}},
		{"collector env", func(p *corev1.Pod) {
			p.Spec.Containers[1].Env = append(p.Spec.Containers[1].Env, corev1.EnvVar{Name: "X", Value: "y"})
		}},
		{"working directory", func(p *corev1.Pod) { p.Spec.Containers[0].WorkingDir = "/" }},
		{"mount path", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].MountPath = "/w" }},
		{"helper writable", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[1].ReadOnly = false }},
		{"collector workspace writable", func(p *corev1.Pod) { p.Spec.Containers[1].VolumeMounts[0].ReadOnly = false }},
		{"sub path", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPath = "x" }},
		{"extra mount", func(p *corev1.Pod) {
			p.Spec.InitContainers[0].VolumeMounts = append(p.Spec.InitContainers[0].VolumeMounts,
				corev1.VolumeMount{Name: "output-token", MountPath: "/run/skygo/output", ReadOnly: true})
		}},
		{"resources", func(p *corev1.Pod) {
			p.Spec.Containers[1].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		}},
		{"lifecycle", func(p *corev1.Pod) {
			p.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"x"}}}}
		}},
		{"probe", func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"x"}}}}
		}},
		{"tty", func(p *corev1.Pod) { p.Spec.Containers[0].TTY = true }},
		{"stdin", func(p *corev1.Pod) { p.Spec.Containers[0].Stdin = true }},
		{"added volume", func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "extra", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		}},
		{"hostPath volume", func(p *corev1.Pod) {
			p.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}
		}},
		{"emptyDir medium", func(p *corev1.Pod) { p.Spec.Volumes[0].EmptyDir.Medium = corev1.StorageMediumMemory }},
		{"secret mode", func(p *corev1.Pod) {
			mode := int32(0o644)
			p.Spec.Volumes[2].Secret.DefaultMode = &mode
		}},
		{"secret name", func(p *corev1.Pod) { p.Spec.Volumes[2].Secret.SecretName = "other" }},
		{"alias read-only", func(p *corev1.Pod) { p.Spec.Volumes[4].PersistentVolumeClaim.ReadOnly = false }},
		{"privileged", func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.Privileged = &yes }},
		{"run as root", func(p *corev1.Pod) { p.Spec.InitContainers[0].SecurityContext.RunAsUser = &zero }},
		{"capability added", func(p *corev1.Pod) {
			p.Spec.Containers[1].SecurityContext.Capabilities.Add = []corev1.Capability{"NET_ADMIN"}
		}},
		{"writable rootfs", func(p *corev1.Pod) {
			no := false
			p.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem = &no
		}},
		{"pod security context", func(p *corev1.Pod) { p.Spec.SecurityContext.SupplementalGroups = []int64{0} }},
		{"missing security context", func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext = nil }},
		{"share process namespace", func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = &yes }},
		{"host pid", func(p *corev1.Pod) { p.Spec.HostPID = true }},
		{"host network", func(p *corev1.Pod) { p.Spec.HostNetwork = true }},
		{"host ipc", func(p *corev1.Pod) { p.Spec.HostIPC = true }},
		{"automount token", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = &yes }},
		{"automount unset", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = nil }},
		{"service links", func(p *corev1.Pod) { p.Spec.EnableServiceLinks = &yes }},
		{"restart policy", func(p *corev1.Pod) { p.Spec.RestartPolicy = corev1.RestartPolicyOnFailure }},
		{"deadline", func(p *corev1.Pod) {
			d := *p.Spec.ActiveDeadlineSeconds + 1
			p.Spec.ActiveDeadlineSeconds = &d
		}},
		{"node selector", func(p *corev1.Pod) { p.Spec.NodeSelector["extra"] = "x" }},
		{"affinity", func(p *corev1.Pod) { p.Spec.Affinity = &corev1.Affinity{} }},
		{"runtime class", func(p *corev1.Pod) {
			rc := "other"
			p.Spec.RuntimeClassName = &rc
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := observed()
			tt.mutate(got)
			if err := b.dataPodMatches(want, got); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("dataPodMatches = %v, want ErrIdentityMismatch", err)
			}
		})
	}
	if err := b.dataPodMatches(want, nil); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("dataPodMatches(nil) = %v", err)
	}
}

func TestDataSecret(t *testing.T) {
	b := profileBackend()
	token := []byte(profileToken)
	s := b.dataSecret(profileID, "input", token, profileHash)
	token[0] = 'X'
	if string(s.Data[tokenKey]) != profileToken {
		t.Fatal("dataSecret retained the caller's token slice")
	}
	if s.Name != "skygo-"+profileID+"-input" || s.Namespace != "jobs" || s.Type != corev1.SecretTypeOpaque ||
		s.Immutable == nil || !*s.Immutable || len(s.Data) != 1 || len(s.StringData) != 0 ||
		len(s.OwnerReferences) != 0 ||
		!reflect.DeepEqual(s.Labels, map[string]string{ownerLabel: "owner", idLabel: profileID}) ||
		!reflect.DeepEqual(s.Annotations, map[string]string{taskAnnotation: profileHash,
			bindingAnnotation: "sha256:binding", layoutAnnotation: dataLayout}) {
		t.Fatalf("secret = %+v", s)
	}
	if got := dataSecretName(profileID, "output"); got != "skygo-"+profileID+"-output" {
		t.Fatalf("dataSecretName = %q", got)
	}
}

func TestDataTokenConfinement(t *testing.T) {
	b := profileBackend()
	task := profileTask()
	task.Workdir = profileWorkdir()
	task.Setup = []string{"make"}
	task.Outputs = []string{"out"}
	p, err := b.dataPod(profileID, task, profileBudgets(t), profileHash)
	if err != nil {
		t.Fatal(err)
	}
	s := b.dataSecret(profileID, "output", []byte(profileToken), profileHash)
	s.Data = nil
	for _, v := range []any{p, s} {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{profileToken, base64.StdEncoding.EncodeToString([]byte(profileToken))} {
			if bytes.Contains(raw, []byte(secret)) {
				t.Fatalf("token bytes outside Secret.Data in %T", v)
			}
		}
	}
	for _, c := range profileContainers(p) {
		for _, e := range c.Env {
			if strings.Contains(e.Name, "TOKEN") {
				t.Errorf("%s env names a token: %s", c.Name, e.Name)
			}
		}
	}
}

func TestDataSecretMatches(t *testing.T) {
	b := profileBackend()
	want := b.dataSecret(profileID, "input", []byte(profileToken), profileHash)
	observed := func() *corev1.Secret {
		s := want.DeepCopy()
		s.UID = "uid"
		s.ResourceVersion = "1"
		s.Annotations["example.com/note"] = "x"
		s.Data[tokenKey] = bytes.Repeat([]byte{'z'}, len(profileToken))
		return s
	}
	if err := b.dataSecretMatches(want, observed()); err != nil {
		t.Fatalf("dataSecretMatches(observed) = %v", err)
	}
	no := false
	tests := []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{"name", func(s *corev1.Secret) { s.Name = dataSecretName(profileID, "output") }},
		{"namespace", func(s *corev1.Secret) { s.Namespace = "other" }},
		{"extra label", func(s *corev1.Secret) { s.Labels["extra"] = "x" }},
		{"owner label", func(s *corev1.Secret) { s.Labels[ownerLabel] = "other" }},
		{"binding annotation", func(s *corev1.Secret) { s.Annotations[bindingAnnotation] = "other" }},
		{"skygo annotation", func(s *corev1.Secret) { s.Annotations["skygo.tmc.dev/extra"] = "x" }},
		{"owner reference", func(s *corev1.Secret) {
			s.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "x", UID: "u"}}
		}},
		{"type", func(s *corev1.Secret) { s.Type = corev1.SecretTypeServiceAccountToken }},
		{"mutable", func(s *corev1.Secret) { s.Immutable = &no }},
		{"immutable unset", func(s *corev1.Secret) { s.Immutable = nil }},
		{"extra key", func(s *corev1.Secret) { s.Data["other"] = []byte("x") }},
		{"missing token", func(s *corev1.Secret) { s.Data = map[string][]byte{"other": []byte(profileToken)} }},
		{"token length", func(s *corev1.Secret) { s.Data[tokenKey] = s.Data[tokenKey][1:] }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := observed()
			tt.mutate(got)
			if err := b.dataSecretMatches(want, got); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("dataSecretMatches = %v, want ErrIdentityMismatch", err)
			}
		})
	}
}

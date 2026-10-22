package kube

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/rest"
)

var errQualificationCoverage = errors.New("kubernetes qualification coverage differs")

// qualificationPathsID identifies source-defined paths, not a native path
// observation. The future producer must separately exercise receiving images.
func qualificationPathsID() string {
	fields := []string{dataLayout, dataProfileVersion, qualificationComposition,
		workspaceMount, stagePath, workPath, helperMount, helperCopy, helperEntry,
		credentialScope, inputTokenDir, outputTokenDir,
		fmt.Sprint(dataUser), fmt.Sprint(profileSecretMode)}
	fields = append(fields, protectedPaths()...)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(fields, "\n")+"\n")))
}

// validateQualificationCoverage compares trusted current configuration/build
// facts and returns a detached record. It neither acquires those facts nor
// constructs a Backend, and cannot establish that native cases actually ran.
func validateQualificationCoverage(r *qualificationRecord, t TargetConfig, config *rest.Config, actual qualificationImplementation) (*qualificationRecord, error) {
	if _, err := encodeQualificationRecord(r); err != nil {
		return nil, err
	}
	if err := validateTarget(t); err != nil {
		return nil, errQualificationCoverage
	}
	d, err := newDataProfile(t.Data)
	if err != nil || d == nil || config == nil || config.CAFile != "" || len(config.CAData) == 0 || config.Insecure {
		return nil, errQualificationCoverage
	}
	if r.Implementation != actual || actual.HelperImage != d.helperImage || actual.HelperSHA256 != d.helperSHA256 || actual.HelperOS != "linux" {
		return nil, errQualificationCoverage
	}
	raw, err := bindingJSON(t, config, d, nil)
	if err != nil || r.TargetSHA256 != fmt.Sprintf("%x", sha256.Sum256(raw)) {
		return nil, errQualificationCoverage
	}
	e := r.Deployment
	if e.APIServer != config.Host || e.CASHA256 != fmt.Sprintf("%x", sha256.Sum256(config.CAData)) || e.Namespace != t.Namespace || !slices.Contains(t.Resources.NodeSelectors[e.Pool.Key], e.Pool.Value) {
		return nil, errQualificationCoverage
	}
	for _, node := range e.Nodes {
		if node.OS != actual.HelperOS || node.Architecture != actual.HelperArchitecture {
			return nil, errQualificationCoverage
		}
	}
	for _, storage := range e.Storage {
		ref, ok := d.references[storage.Reference]
		if !ok || ref.pvc == "" {
			return nil, errQualificationCoverage
		}
	}
	workdir, mountOnly, pvc, secret := false, false, false, false
	for _, image := range r.Images {
		if !slices.Contains(d.taskImages, image.Image) || image.OS != actual.HelperOS || image.Architecture != actual.HelperArchitecture || image.FixedPathsSHA256 != qualificationPathsID() {
			return nil, errQualificationCoverage
		}
		hasMountOnly := slices.Contains(image.Profiles, "mount-only")
		mountOnly = mountOnly || hasMountOnly
		if hasMountOnly && !image.DefaultDirectory && len(image.Directories) == 0 || !hasMountOnly && (image.DefaultDirectory || len(image.Directories) != 0) {
			return nil, errQualificationCoverage
		}
		for _, profile := range image.Profiles {
			workdir = workdir || profile != "mount-only"
		}
		for _, directory := range image.Directories {
			for _, protected := range protectedPaths() {
				if pathsOverlap(directory, protected) {
					return nil, errQualificationCoverage
				}
			}
		}
		for _, m := range image.Mounts {
			ref, ok := d.references[m.Reference]
			if !ok || !slices.Contains(ref.destinations, m.Destination) || !m.ReadOnly && (ref.pvc == "" || !ref.allowWrite) {
				return nil, errQualificationCoverage
			}
			if ref.pvc != "" {
				pvc = true
				if !slices.ContainsFunc(e.Storage, func(s qualificationStorage) bool { return s.Reference == m.Reference }) {
					return nil, errQualificationCoverage
				}
			} else {
				secret = true
			}
		}
	}
	required := []string{"normal-retirement", "paths-security", "worker-tls-credentials"}
	if workdir {
		required = append(required, "input-output-digests", "setup-before-main", "stage-no-launch", "setup-no-launch", "main-failure", "long-task-renewal")
	}
	if mountOnly {
		required = append(required, "mount-only")
	}
	if pvc {
		required = append(required, "pvc-persistence")
	}
	if secret {
		required = append(required, "secret-projection", "secret-missing-key")
	}
	for _, name := range required {
		if !slices.ContainsFunc(r.Evidence.Cases, func(c qualificationCase) bool { return c.Name == name }) {
			return nil, errQualificationCoverage
		}
	}
	return cloneQualificationRecord(r), nil
}

// checkQualificationTask requires an already validated immutable record and
// the constructor's frozen data profile/policy. It does not observe a cluster,
// install qualification, or waive existing Backend.Check checks.
func checkQualificationTask(r *qualificationRecord, d *dataProfile, policy ResourcePolicy, t backend.Task) error {
	if r == nil || d == nil || t.Retry != nil || r.Composition != qualificationComposition || r.Implementation.HelperImage != d.helperImage || r.Implementation.HelperSHA256 != d.helperSHA256 {
		return errQualificationCoverage
	}
	if err := backend.ValidateDataTask(t, protectedPaths()); err != nil {
		return err
	}
	if _, err := backend.DefaultDataPolicy().Budgets(t.TimeoutSeconds); err != nil {
		return err
	}
	if err := (&Backend{policy: policy}).checkPolicy(t); err != nil {
		return err
	}
	pool := r.Deployment.Pool
	if pool.Key == "" || pool.Value == "" || t.NodeSelector[pool.Key] != pool.Value {
		return errQualificationCoverage
	}
	if !slices.Contains(d.taskImages, t.Image) {
		return errQualificationCoverage
	}
	index := slices.IndexFunc(r.Images, func(i qualificationImage) bool { return i.Image == t.Image })
	if index < 0 {
		return errQualificationCoverage
	}
	image := r.Images[index]
	profile := "mount-only"
	if t.Workdir != nil {
		profile = "workdir"
		if len(t.Setup) != 0 {
			profile += "-setup"
		}
		if len(t.Outputs) != 0 {
			profile += "-output"
		}
	}
	if !slices.Contains(image.Profiles, profile) {
		return errQualificationCoverage
	}
	if t.Workdir == nil && (t.Directory == "" && !image.DefaultDirectory || t.Directory != "" && !slices.Contains(image.Directories, t.Directory)) {
		return errQualificationCoverage
	}
	for _, m := range t.DataMounts {
		ref, ok := d.references[m.Reference]
		if !ok || !slices.Contains(ref.destinations, m.Destination) || !m.ReadOnly && (ref.pvc == "" || !ref.allowWrite) || !slices.Contains(image.Mounts, qualificationMount{Reference: m.Reference, Destination: m.Destination, ReadOnly: m.ReadOnly}) {
			return errQualificationCoverage
		}
	}
	return nil
}

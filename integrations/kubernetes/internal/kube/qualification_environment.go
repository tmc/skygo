package kube

import "slices"

// compareQualificationEnvironment compares typed actual observations and the
// operator's explicitly maintained policy facts. It performs no requests and
// supplies no completion, HTTP-body or native receipt. The integration caller
// must collect bounded observations in its SAME original owned request frame.
// In particular, copying a record into actual is not environment observation.
func compareQualificationEnvironment(r *qualificationRecord, actual qualificationDeployment) error {
	if r == nil {
		return errQualificationCoverage
	}
	// Reuse the record codec's bounds and ordering checks for the input facts.
	candidate := *r
	candidate.Deployment = actual
	if _, err := encodeQualificationRecord(&candidate); err != nil {
		return errQualificationCoverage
	}
	want := r.Deployment
	if want.APIServer != actual.APIServer || want.CASHA256 != actual.CASHA256 || want.Namespace != actual.Namespace || want.NamespaceUID != actual.NamespaceUID || want.KubernetesVersion != actual.KubernetesVersion || want.Pool != actual.Pool || want.PolicyRevision != actual.PolicyRevision || !slices.Equal(want.Nodes, actual.Nodes) || !slices.Equal(want.Storage, actual.Storage) {
		return errQualificationCoverage
	}
	return nil
}

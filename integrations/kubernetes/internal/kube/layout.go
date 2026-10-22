package kube

// The fixed skygo-data-layout-v1 paths. They are constants, not operator or
// task configuration; an incompatible change requires a new layout identifier.
const (
	dataLayout = "skygo-data-layout-v1"

	workspaceMount  = "/workspace"
	stagePath       = "/workspace/.stage"
	workPath        = "/workspace/work"
	helperMount     = "/opt/skygo"
	helperCopy      = "/opt/skygo/bin/skygo-data"
	helperEntry     = "/skygo-data"
	credentialScope = "/run/skygo"
	inputTokenDir   = "/run/skygo/input"
	outputTokenDir  = "/run/skygo/output"
	tokenKey        = "token"

	// dataUser is the fixed nonroot UID, GID and fsGroup of every container.
	dataUser = 65532
)

// protectedPaths returns the complete trusted argument to
// backend.ValidateDataTask. It covers every profile, including scopes absent
// from a particular Pod, so one list protects all supported layouts.
func protectedPaths() []string {
	return []string{workspaceMount, helperMount, helperEntry, credentialScope,
		"/proc", "/sys", "/dev", "/var/run/secrets", "/run/secrets"}
}

// dataServiceRoute is the generated worker Connect service path. Allocating it
// grants no capability; only an installed qualification does.
const dataServiceRoute = "/skygo.v1.RemoteDataService/"

// dataServiceMethods returns the exact generated procedure paths of
// dataServiceRoute, sorted.
func dataServiceMethods() []string {
	return []string{
		"/skygo.v1.RemoteDataService/BeginOutput",
		"/skygo.v1.RemoteDataService/FinishOutput",
		"/skygo.v1.RemoteDataService/PollRelease",
		"/skygo.v1.RemoteDataService/ReadInput",
		"/skygo.v1.RemoteDataService/WriteOutput",
	}
}

// dataProfileVersion identifies the Pod builder and helper command, environment
// and mount protocol of profile.go, together with the service route. Any change
// to them requires a new version.
const dataProfileVersion = "skygo-data-profile-v2"

// dataReplacementPolicy records that aliases name existing PVCs and Secrets
// whose replacement is governed by operator RBAC, not pinned by UID or content.
const dataReplacementPolicy = "name-based-operator-trusted-v1"

// dataProfile is the frozen data configuration of one target. It is built once
// from DataConfig by newDataProfile and never mutated afterwards.
type dataProfile struct {
	helperImage  string // digest-pinned helper image reference
	helperSHA256 string // expected helper executable digest, lowercase hex
	origin       string // canonical https origin
	host         string // TLS server name, the origin's hostname
	caPEM        []byte // exact configured certificate bytes
	caSHA256     string // lowercase hex digest of caPEM
	taskImages   []string
	references   map[string]dataReference
}

// dataReference is one operator alias for an existing namespace volume.
// Exactly one of pvc and secret is set; keys are only for secret.
type dataReference struct {
	pvc, secret  string
	keys         []string
	destinations []string
	allowWrite   bool
}

// dataQualification is a target's installed native qualification. Only the
// constructor creates one, after it validated the operator's record against
// the actual build and target and compared the record's deployment with
// positive startup reads (see qualification_live.go). Production targets
// without a configured record leave it nil, so Check refuses every data task.
// Tests may construct one only as a labeled model fixture.
type dataQualification struct {
	id      string               // qualificationRecordID of record
	record  *qualificationRecord // immutable validated snapshot
	premise qualificationPremise // facts the API cannot supply
}

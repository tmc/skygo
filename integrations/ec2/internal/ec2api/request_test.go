package ec2api

import (
	"bytes"
	"strings"
	"testing"
)

// goldenCreate is the body for testProfile, testIdentity and user data
// "#cloud-config\n", computed by an independent script from the field
// list and token and profile hash definitions, not from this package.
var goldenCreate = strings.Join([]string{
	"Action=RunInstances",
	"BlockDeviceMapping.1.DeviceName=%2Fdev%2Fxvda",
	"BlockDeviceMapping.1.Ebs.DeleteOnTermination=true",
	"BlockDeviceMapping.1.Ebs.Encrypted=true",
	"BlockDeviceMapping.1.Ebs.VolumeSize=16",
	"BlockDeviceMapping.1.Ebs.VolumeType=gp3",
	"ClientToken=skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0",
	"ImageId=ami-0123456789abcdef0",
	"InstanceInitiatedShutdownBehavior=terminate",
	"InstanceType=t3.micro",
	"MaxCount=1",
	"MetadataOptions.HttpEndpoint=enabled",
	"MetadataOptions.HttpPutResponseHopLimit=1",
	"MetadataOptions.HttpTokens=required",
	"MinCount=1",
	"SecurityGroupId.1=sg-0aaaaaaaaaaaaaaaa",
	"SecurityGroupId.2=sg-0bbbbbbbbbbbbbbbb",
	"SubnetId=subnet-0123456789abcdef0",
	"TagSpecification.1.ResourceType=instance",
	"TagSpecification.1.Tag.1.Key=Name",
	"TagSpecification.1.Tag.1.Value=skygo-env-0123",
	"TagSpecification.1.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.1.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.1.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.1.Tag.3.Value=1",
	"TagSpecification.1.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.1.Tag.4.Value=install-7f3a",
	"TagSpecification.1.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.1.Tag.5.Value=b85c987d09a77a0bacd2bed09255a284b8c768fc6e454da9e67a17c8210f3e4d",
	"TagSpecification.2.ResourceType=volume",
	"TagSpecification.2.Tag.1.Key=Name",
	"TagSpecification.2.Tag.1.Value=skygo-env-0123",
	"TagSpecification.2.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.2.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.2.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.2.Tag.3.Value=1",
	"TagSpecification.2.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.2.Tag.4.Value=install-7f3a",
	"TagSpecification.2.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.2.Tag.5.Value=b85c987d09a77a0bacd2bed09255a284b8c768fc6e454da9e67a17c8210f3e4d",
	"TagSpecification.3.ResourceType=network-interface",
	"TagSpecification.3.Tag.1.Key=Name",
	"TagSpecification.3.Tag.1.Value=skygo-env-0123",
	"TagSpecification.3.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.3.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.3.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.3.Tag.3.Value=1",
	"TagSpecification.3.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.3.Tag.4.Value=install-7f3a",
	"TagSpecification.3.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.3.Tag.5.Value=b85c987d09a77a0bacd2bed09255a284b8c768fc6e454da9e67a17c8210f3e4d",
	"UserData=I2Nsb3VkLWNvbmZpZwo%3D",
	"Version=2016-11-15",
}, "&")

// goldenBudget is the body for a profile shaped like root's native budget
// selection: one t4g.small with standard CPU credits and one encrypted 8 GiB
// gp3 root volume with baseline performance. The account, subnet, group,
// image and key values are fixtures. Computed by the same independent script
// as goldenCreate.
var goldenBudget = strings.Join([]string{
	"Action=RunInstances",
	"BlockDeviceMapping.1.DeviceName=%2Fdev%2Fxvda",
	"BlockDeviceMapping.1.Ebs.DeleteOnTermination=true",
	"BlockDeviceMapping.1.Ebs.Encrypted=true",
	"BlockDeviceMapping.1.Ebs.VolumeSize=8",
	"BlockDeviceMapping.1.Ebs.VolumeType=gp3",
	"ClientToken=skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0",
	"CreditSpecification.CpuCredits=standard",
	"ImageId=ami-0fedcba9876543210",
	"InstanceInitiatedShutdownBehavior=terminate",
	"InstanceType=t4g.small",
	"KeyName=fixture-key",
	"MaxCount=1",
	"MetadataOptions.HttpEndpoint=enabled",
	"MetadataOptions.HttpPutResponseHopLimit=1",
	"MetadataOptions.HttpTokens=required",
	"MinCount=1",
	"SecurityGroupId.1=sg-0aaaaaaaaaaaaaaaa",
	"SubnetId=subnet-0123456789abcdef0",
	"TagSpecification.1.ResourceType=instance",
	"TagSpecification.1.Tag.1.Key=Name",
	"TagSpecification.1.Tag.1.Value=skygo-env-0123",
	"TagSpecification.1.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.1.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.1.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.1.Tag.3.Value=1",
	"TagSpecification.1.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.1.Tag.4.Value=install-7f3a",
	"TagSpecification.1.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.1.Tag.5.Value=2188b74c6db990b9020494c833372648768b46aa6e29c202662ddeb71dc78be0",
	"TagSpecification.2.ResourceType=volume",
	"TagSpecification.2.Tag.1.Key=Name",
	"TagSpecification.2.Tag.1.Value=skygo-env-0123",
	"TagSpecification.2.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.2.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.2.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.2.Tag.3.Value=1",
	"TagSpecification.2.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.2.Tag.4.Value=install-7f3a",
	"TagSpecification.2.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.2.Tag.5.Value=2188b74c6db990b9020494c833372648768b46aa6e29c202662ddeb71dc78be0",
	"TagSpecification.3.ResourceType=network-interface",
	"TagSpecification.3.Tag.1.Key=Name",
	"TagSpecification.3.Tag.1.Value=skygo-env-0123",
	"TagSpecification.3.Tag.2.Key=skygo.tmc.dev%2Fenvironment",
	"TagSpecification.3.Tag.2.Value=env-0123456789abcdef",
	"TagSpecification.3.Tag.3.Key=skygo.tmc.dev%2Fgeneration",
	"TagSpecification.3.Tag.3.Value=1",
	"TagSpecification.3.Tag.4.Key=skygo.tmc.dev%2Fowner",
	"TagSpecification.3.Tag.4.Value=install-7f3a",
	"TagSpecification.3.Tag.5.Key=skygo.tmc.dev%2Fprofile",
	"TagSpecification.3.Tag.5.Value=2188b74c6db990b9020494c833372648768b46aa6e29c202662ddeb71dc78be0",
	"Version=2016-11-15",
}, "&")

func TestCreateRequestBudgetShape(t *testing.T) {
	p := &Profile{
		Account:          "123456789012",
		Region:           "us-east-1",
		SubnetID:         "subnet-0123456789abcdef0",
		SecurityGroupIDs: []string{"sg-0aaaaaaaaaaaaaaaa"},
		ImageID:          "ami-0fedcba9876543210",
		InstanceType:     "t4g.small",
		CPUCredits:       "standard",
		KeyName:          "fixture-key",
		RootDevice:       "/dev/xvda",
		RootVolumeGiB:    8,
		RootVolumeType:   "gp3",
		EncryptRoot:      true,
	}
	req, err := NewCreateRequest(p, testIdentity(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(req.Body()); got != goldenBudget {
		t.Fatalf("body differs from golden:\n got %s\nwant %s", got, goldenBudget)
	}
	for _, absent := range []string{"Iops", "Throughput", "IamInstanceProfile", "InstanceMarketOptions", "UserData"} {
		if strings.Contains(goldenBudget, absent) {
			t.Errorf("body contains %s", absent)
		}
	}
	parsed, err := ParseCreateRequest(req.Body())
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.matches(p); err != nil {
		t.Fatal(err)
	}
	q := *p
	q.CPUCredits = "unlimited"
	if err := parsed.matches(&q); err == nil {
		t.Fatal("request matched a profile with unlimited CPU credits")
	}
	q.CPUCredits = "burst"
	if err := q.Validate(); err == nil {
		t.Fatal("invalid CPU credit option accepted")
	}
}

func TestCreateRequestGolden(t *testing.T) {
	req := testRequest(t)
	if got := string(req.Body()); got != goldenCreate {
		t.Fatalf("body differs from golden:\n got %s\nwant %s", got, goldenCreate)
	}
	if got, want := req.ClientToken(), "skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0"; got != want {
		t.Errorf("ClientToken = %s, want %s", got, want)
	}
	if got := req.ProfileHash(); got != testProfile().Hash() {
		t.Errorf("ProfileHash = %s, want %s", got, testProfile().Hash())
	}
	if got := req.Identity(); got != testIdentity() {
		t.Errorf("Identity = %+v, want %+v", got, testIdentity())
	}
}

func TestCreateRequestImmutable(t *testing.T) {
	ud := []byte("#cloud-config\n")
	req, err := NewCreateRequest(testProfile(), testIdentity(), ud)
	if err != nil {
		t.Fatal(err)
	}
	ud[0] = 'X'
	b := req.Body()
	b[0] = 'X'
	if string(req.Body()) != goldenCreate {
		t.Fatal("request changed through its inputs or Body result")
	}
}

func TestCreateRequestLease(t *testing.T) {
	id := testIdentity()
	id.Lease = "s12-native-1"
	req, err := NewCreateRequest(testProfile(), id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if req.ClientToken() != testIdentity().ClientToken() {
		t.Error("lease changed the client token")
	}
	body := string(req.Body())
	for _, want := range []string{
		"TagSpecification.3.Tag.1.Value=skygo-s12-native-1-env-0123",
		"Tag.4.Key=skygo.tmc.dev%2Flease&",
		"TagSpecification.3.Tag.4.Value=s12-native-1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q", want)
		}
	}
	if strings.Contains(body, "UserData") {
		t.Error("empty user data was encoded")
	}
	parsed, err := ParseCreateRequest(req.Body())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Identity() != id {
		t.Errorf("Identity = %+v, want %+v", parsed.Identity(), id)
	}
}

func TestParseCreateRequestRoundTrip(t *testing.T) {
	req, err := ParseCreateRequest([]byte(goldenCreate))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(req.Body(), []byte(goldenCreate)) || req.ClientToken() != testRequest(t).ClientToken() {
		t.Fatal("parsed request differs")
	}
	if err := req.matches(testProfile()); err != nil {
		t.Fatal(err)
	}
}

func TestParseCreateRequestRejects(t *testing.T) {
	replace := func(old, new string) string {
		if !strings.Contains(goldenCreate, old) {
			t.Fatalf("golden lacks %q", old)
		}
		return strings.Replace(goldenCreate, old, new, 1)
	}
	tests := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"token", replace("ClientToken=skygo-55", "ClientToken=skygo-66")},
		{"no volume tags", replace("TagSpecification.2.ResourceType=volume", "TagSpecification.2.ResourceType=instance")},
		{"different volume tag", replace("TagSpecification.2.Tag.3.Value=1", "TagSpecification.2.Tag.3.Value=2")},
		{"shutdown behavior", replace("InstanceInitiatedShutdownBehavior=terminate", "InstanceInitiatedShutdownBehavior=stop")},
		{"imds tokens", replace("HttpTokens=required", "HttpTokens=optional")},
		{"hop limit", replace("HttpPutResponseHopLimit=1", "HttpPutResponseHopLimit=2")},
		{"delete on termination", replace("DeleteOnTermination=true", "DeleteOnTermination=false")},
		{"max count", replace("MaxCount=1", "MaxCount=2")},
		{"extra parameter", goldenCreate + "&IamInstanceProfile.Name=admin"},
		{"repeated parameter", goldenCreate + "&MinCount=1"},
		{"noncanonical number", replace("VolumeSize=16", "VolumeSize=016")},
		{"unsorted", "Version=2016-11-15&" + strings.TrimSuffix(goldenCreate, "&Version=2016-11-15")},
		{"generation tag", replace("Tag.3.Value=1&", "Tag.3.Value=2&")},
		{"user data", replace("UserData=I2Nsb3VkLWNvbmZpZwo%3D", "UserData=I2Nsb3VkLWNvbmZpZwo")},
		{"too large", goldenCreate + "&" + strings.Repeat("x", MaxCreateBody)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseCreateRequest([]byte(tt.body)); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestCreateRequestMatches(t *testing.T) {
	req := testRequest(t)
	reordered := testProfile()
	reordered.SecurityGroupIDs = []string{"sg-0aaaaaaaaaaaaaaaa", "sg-0bbbbbbbbbbbbbbbb"}
	if err := req.matches(reordered); err != nil {
		t.Fatalf("security group order mattered: %v", err)
	}
	for _, change := range []func(*Profile){
		func(p *Profile) { p.Account = "210987654321" },
		func(p *Profile) { p.Region = "us-east-1" },
		func(p *Profile) { p.RootVolumeGiB = 17 },
		func(p *Profile) { p.EncryptRoot = false },
		func(p *Profile) { p.SecurityGroupIDs = p.SecurityGroupIDs[:1] },
	} {
		p := testProfile()
		change(p)
		if err := req.matches(p); err == nil {
			t.Errorf("request matched changed profile %+v", p)
		}
	}
}

func TestNewCreateRequestRejects(t *testing.T) {
	if _, err := NewCreateRequest(testProfile(), testIdentity(), make([]byte, MaxUserData+1)); err == nil {
		t.Error("oversized user data accepted")
	}
	if _, err := NewCreateRequest(testProfile(), Identity{Owner: "o", Environment: "e"}, nil); err == nil {
		t.Error("zero generation accepted")
	}
	p := testProfile()
	p.ImageID = "ami-1"
	if _, err := NewCreateRequest(p, testIdentity(), nil); err == nil {
		t.Error("invalid profile accepted")
	}
	if _, err := NewCreateRequest(testProfile(), testIdentity(), make([]byte, MaxUserData)); err != nil {
		t.Errorf("maximum user data refused: %v", err)
	}
}

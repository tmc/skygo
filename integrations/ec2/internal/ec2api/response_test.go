package ec2api

import (
	"encoding/base64"
	"encoding/xml"
	"reflect"
	"strings"
	"testing"
)

const testDescribe = `<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>
  <reservationSet>
    <item>
      <reservationId>r-1234567890abcdef0</reservationId>
      <ownerId>123456789012</ownerId>
      <groupSet/>
      <instancesSet>
        <item>
          <instanceId>i-1234567890abcdef0</instanceId>
          <imageId>ami-0123456789abcdef0</imageId>
          <instanceState><code>16</code><name>running</name></instanceState>
          <privateDnsName>ip-10-0-0-5.us-west-2.compute.internal</privateDnsName>
          <instanceType>t3.micro</instanceType>
          <placement><availabilityZone>us-west-2a</availabilityZone><tenancy>default</tenancy></placement>
          <subnetId>subnet-0123456789abcdef0</subnetId>
          <vpcId>vpc-0123456789abcdef0</vpcId>
          <privateIpAddress>10.0.0.5</privateIpAddress>
          <ipAddress>203.0.113.7</ipAddress>
          <groupSet>
            <item><groupId>sg-0bbbbbbbbbbbbbbbb</groupId><groupName>b</groupName></item>
            <item><groupId>sg-0aaaaaaaaaaaaaaaa</groupId><groupName>a</groupName></item>
          </groupSet>
          <clientToken>skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0</clientToken>
          <tagSet>
            <item><key>skygo.tmc.dev/owner</key><value>install-7f3a</value></item>
            <item><key>Name</key><value>skygo-env-0123</value></item>
          </tagSet>
          <metadataOptions><httpTokens>required</httpTokens></metadataOptions>
        </item>
      </instancesSet>
    </item>
  </reservationSet>
  <nextToken>abc</nextToken>
</DescribeInstancesResponse>`

func describeRoot() xml.Name {
	return xml.Name{Space: ec2Namespace, Local: "DescribeInstancesResponse"}
}

func TestDecodeDescribe(t *testing.T) {
	var out describeResponse
	if err := decodeXML([]byte(testDescribe), describeRoot(), &out); err != nil {
		t.Fatal(err)
	}
	rs, err := out.reservations()
	if err != nil {
		t.Fatal(err)
	}
	if out.NextToken != "abc" || len(rs) != 1 || len(rs[0].Instances) != 1 {
		t.Fatalf("decoded %+v", out)
	}
	inst := rs[0].Instances[0]
	want := Instance{
		ID:               "i-1234567890abcdef0",
		State:            "running",
		ImageID:          "ami-0123456789abcdef0",
		InstanceType:     "t3.micro",
		SubnetID:         "subnet-0123456789abcdef0",
		VPCID:            "vpc-0123456789abcdef0",
		AvailabilityZone: "us-west-2a",
		ClientToken:      "skygo-55e39499ed37635201145febc23225f7968c94e1b96a477577cc0dedc0",
		SecurityGroupIDs: []string{"sg-0aaaaaaaaaaaaaaaa", "sg-0bbbbbbbbbbbbbbbb"},
		Tags:             []Tag{{"Name", "skygo-env-0123"}, {"skygo.tmc.dev/owner", "install-7f3a"}},
		PublicIP:         "203.0.113.7",
		PrivateIP:        "10.0.0.5",
	}
	if rs[0].OwnerID != "123456789012" || rs[0].ID != "r-1234567890abcdef0" {
		t.Errorf("reservation = %+v", rs[0])
	}
	if !reflect.DeepEqual(inst, want) {
		t.Errorf("instance = %+v\nwant %+v", inst, want)
	}
}

func TestCheckXMLRejects(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{"wrong root", strings.Replace(testDescribe, "DescribeInstancesResponse", "RunInstancesResponse", 2)},
		{"wrong namespace", strings.Replace(testDescribe, "doc/2016-11-15/", "doc/2015-10-01/", 1)},
		{"repeated instance ID", strings.Replace(testDescribe, "<imageId>", "<instanceId>i-0fffffffffffffff0</instanceId><imageId>", 1)},
		{"repeated owner", strings.Replace(testDescribe, "<groupSet/>", "<ownerId>210987654321</ownerId><groupSet/>", 1)},
		{"repeated next token", strings.Replace(testDescribe, "</DescribeInstancesResponse>", "<nextToken>abd</nextToken></DescribeInstancesResponse>", 1)},
		{"repeated client token", strings.Replace(testDescribe, "<tagSet>", "<clientToken>skygo-x</clientToken><tagSet>", 1)},
		{"trailing element", testDescribe + "<x/>"},
		{"trailing text", testDescribe + "x"},
		{"second root", testDescribe + testDescribe[strings.Index(testDescribe, "<Describe"):]},
		{"directive", `<!DOCTYPE x>` + testDescribe[strings.Index(testDescribe, "<Describe"):]},
		{"truncated", testDescribe[:len(testDescribe)-10]},
		{"empty", ""},
		{"deep", `<DescribeInstancesResponse xmlns="` + ec2Namespace + `">` + strings.Repeat("<item>", maxXMLDepth) + strings.Repeat("</item>", maxXMLDepth) + `</DescribeInstancesResponse>`},
		{"many elements", `<DescribeInstancesResponse xmlns="` + ec2Namespace + `">` + strings.Repeat("<item/>", maxXMLElements) + `</DescribeInstancesResponse>`},
		{"too large", `<DescribeInstancesResponse xmlns="` + ec2Namespace + `">` + strings.Repeat(" ", maxResponseBytes) + `</DescribeInstancesResponse>`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkXML([]byte(tt.doc), func(n xml.Name) bool { return n == describeRoot() }); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestInstanceRejects(t *testing.T) {
	tests := []struct {
		name     string
		old, new string
	}{
		{"instance ID", "<instanceId>i-1234567890abcdef0", "<instanceId>i-XYZ"},
		{"state", "<name>running</name>", "<name>gone</name>"},
		{"zone", "us-west-2a<", "us-west-2<"},
		{"group", "sg-0bbbbbbbbbbbbbbbb", "sg-bad"},
		{"repeated tag", "<key>Name</key>", "<key>skygo.tmc.dev/owner</key>"},
		{"empty tag key", "<key>Name</key>", "<key></key>"},
		{"owner", "<ownerId>123456789012</ownerId>", "<ownerId>12345678901</ownerId>"},
		{"reservation", "r-1234567890abcdef0", "r-1"},
		{"long token", "<clientToken>skygo-", "<clientToken>" + strings.Repeat("x", 64)},
		{"address", "203.0.113.7", "203.0.113.7/24"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := strings.Replace(testDescribe, tt.old, tt.new, 1)
			if doc == testDescribe {
				t.Fatalf("fixture lacks %q", tt.old)
			}
			var out describeResponse
			if err := decodeXML([]byte(doc), describeRoot(), &out); err != nil {
				return // rejected while decoding
			}
			if _, err := out.reservations(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestDescribeBounds(t *testing.T) {
	out := describeResponse{NextToken: strings.Repeat("t", maxNextToken+1)}
	if _, err := out.reservations(); err == nil {
		t.Error("oversized next token accepted")
	}
	out = describeResponse{Reservations: make([]xmlReservation, maxReservations+1)}
	if _, err := out.reservations(); err == nil {
		t.Error("too many reservations accepted")
	}
	x := xmlReservation{ID: "r-1234567890abcdef0", OwnerID: "123456789012", Instances: make([]xmlInstance, maxInstances+1)}
	if _, err := x.reservation(); err == nil {
		t.Error("too many instances accepted")
	}
	inst := xmlInstance{ID: "i-1234567890abcdef0", State: xmlState{"running"}, Tags: make([]xmlTag, maxInstanceTags+1)}
	if _, err := inst.instance(); err == nil {
		t.Error("too many tags accepted")
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		status int
		code   string
		want   Class
	}{
		{400, "InvalidInstanceID.NotFound", ClassInstanceNotFound},
		{400, "IdempotentParameterMismatch", ClassIdempotentParameterMismatch},
		{503, "RequestLimitExceeded", ClassThrottled},
		{400, "Throttling", ClassThrottled},
		{500, "InternalError", ClassServerError},
		{503, "", ClassServerError},
		{403, "UnauthorizedOperation", ClassUnauthorized},
		{401, "AuthFailure", ClassUnauthorized},
		{403, "SignatureDoesNotMatch", ClassUnauthorized},
		{400, "ExpiredToken", ClassUnauthorized},
		{500, "InsufficientInstanceCapacity", ClassInsufficientCapacity},
		{400, "InvalidParameterValue", ClassOther},
		{400, "IncorrectInstanceState", ClassOther},
		{307, "", ClassOther},
	}
	for _, tt := range tests {
		if got := classify(tt.status, tt.code); got != tt.want {
			t.Errorf("classify(%d, %q) = %v, want %v", tt.status, tt.code, got, tt.want)
		}
	}
}

func TestErrorText(t *testing.T) {
	e := &Error{Action: "RunInstances", Class: ClassUnauthorized, Code: "UnauthorizedOperation", StatusCode: 403, RequestID: "abc", Sent: true,
		provider: &providerError{"UnauthorizedOperation", "Encoded authorization failure message: SECRET"}}
	if got, want := e.Error(), "ec2api: RunInstances: unauthorized (UnauthorizedOperation) status 403 request abc"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if u := e.Unwrap(); len(u) != 1 || !strings.Contains(u[0].Error(), "SECRET") {
		t.Errorf("provider message not retained: %v", u)
	}
	if ClassMalformed.String() != "malformed response" || Class(99).String() != "Class(99)" {
		t.Error("Class.String")
	}
}

func TestDecodeError(t *testing.T) {
	const (
		ec2Doc = `<?xml version="1.0" encoding="UTF-8"?><Response><Errors><Error><Code>InvalidInstanceID.NotFound</Code><Message>m1</Message></Error><Error><Code>Other</Code></Error></Errors><RequestID>ea966190-f9aa-478e-9ede-example</RequestID></Response>`
		stsDoc = `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>InvalidClientTokenId</Code><Message>m2</Message></Error><RequestId>r-2</RequestId></ErrorResponse>`
	)
	tests := []struct {
		name, envelope, doc    string
		code, message, request string // all empty if refused
	}{
		{"EC2", "Response", ec2Doc, "InvalidInstanceID.NotFound", "m1", "ea966190-f9aa-478e-9ede-example"},
		{"EC2 without request ID", "Response", `<Response><Errors><Error><Code>AuthFailure</Code></Error></Errors></Response>`, "AuthFailure", "", ""},
		{"STS", "ErrorResponse", stsDoc, "InvalidClientTokenId", "m2", "r-2"},
		{"STS without namespace", "ErrorResponse", `<ErrorResponse><Error><Code>ExpiredToken</Code></Error></ErrorResponse>`, "ExpiredToken", "", ""},
		{"EC2 at STS", "ErrorResponse", ec2Doc, "", "", ""},
		{"STS at EC2", "Response", stsDoc, "", "", ""},
		{"EC2 with STS request ID", "Response", `<Response><Errors><Error><Code>Other</Code></Error></Errors><RequestId>r-3</RequestId></Response>`, "", "", ""},
		{"duplicate code", "Response", `<Response><Errors><Error><Code>Other</Code><Code>RequestLimitExceeded</Code></Error></Errors></Response>`, "", "", ""},
		{"duplicate STS code", "ErrorResponse", `<ErrorResponse><Error><Code>Other</Code><Code>Throttling</Code></Error></ErrorResponse>`, "", "", ""},
		{"two STS errors", "ErrorResponse", `<ErrorResponse><Error><Code>Other</Code></Error><Error><Code>Throttling</Code></Error></ErrorResponse>`, "", "", ""},
		{"duplicate Errors", "Response", `<Response><Errors><Error><Code>Other</Code></Error></Errors><Errors><Error><Code>Throttling</Code></Error></Errors></Response>`, "", "", ""},
		{"excess depth", "Response", `<Response><Errors><Error><Code>Other</Code>` + strings.Repeat("<a>", maxXMLDepth) + strings.Repeat("</a>", maxXMLDepth) + `</Error></Errors></Response>`, "", "", ""},
		{"trailing element", "Response", ec2Doc + `<Response/>`, "", "", ""},
		{"trailing text", "Response", ec2Doc + `x`, "", "", ""},
		{"wrong root", "Response", `<Errors><Error><Code>Other</Code></Error></Errors>`, "", "", ""},
		{"invalid code", "Response", `<Response><Errors><Error><Code>bad code!</Code></Error></Errors></Response>`, "", "", ""},
		{"invalid request ID", "Response", `<Response><Errors><Error><Code>Other</Code></Error></Errors><RequestID>bad id!</RequestID></Response>`, "", "", ""},
		{"no error", "Response", `<Response><Errors/></Response>`, "", "", ""},
		{"not XML", "Response", `<html>not xml`, "", "", ""},
		{"too large", "Response", `<Response><Errors><Error><Code>Other</Code></Error></Errors>` + strings.Repeat(" ", maxResponseBytes) + `</Response>`, "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, message, request, err := decodeError([]byte(tt.doc), tt.envelope)
			if (err == nil) != (tt.code != "") || code != tt.code || message != tt.message || request != tt.request {
				t.Fatalf("decodeError = %q, %q, %q, %v; want %q, %q, %q", code, message, request, err, tt.code, tt.message, tt.request)
			}
		})
	}
}

func TestStateChange(t *testing.T) {
	item := `<item><instanceId>i-1234567890abcdef0</instanceId><currentState><code>64</code><name>stopping</name></currentState><previousState><code>16</code><name>running</name></previousState></item>`
	decode := func(items string) *stateChangeResponse {
		var out stateChangeResponse
		doc := `<StopInstancesResponse xmlns="` + ec2Namespace + `"><instancesSet>` + items + `</instancesSet></StopInstancesResponse>`
		if err := decodeXML([]byte(doc), xml.Name{Space: ec2Namespace, Local: "StopInstancesResponse"}, &out); err != nil {
			t.Fatal(err)
		}
		return &out
	}
	sc, err := decode(item).stateChange("i-1234567890abcdef0")
	if err != nil || *sc != (StateChange{"i-1234567890abcdef0", "running", "stopping"}) {
		t.Fatalf("stateChange = %+v, %v", sc, err)
	}
	if _, err := decode(item).stateChange("i-0234567890abcdef0"); err == nil {
		t.Error("other instance accepted")
	}
	if _, err := decode(item + item).stateChange("i-1234567890abcdef0"); err == nil {
		t.Error("two items accepted")
	}
	if _, err := decode(strings.Replace(item, "stopping", "gone", 1)).stateChange("i-1234567890abcdef0"); err == nil {
		t.Error("invalid state accepted")
	}
}

func TestConsoleResponseOutput(t *testing.T) {
	id := "i-1234567890abcdef0"
	x := consoleResponse{ID: id, Output: base64.StdEncoding.EncodeToString([]byte("hello\n"))}
	if out, err := x.output(id); err != nil || string(out) != "hello\n" {
		t.Fatalf("output = %q, %v", out, err)
	}
	for _, bad := range []consoleResponse{
		{ID: "i-0234567890abcdef0", Output: x.Output},
		{ID: id, Output: "!!!"},
		{ID: id, Output: base64.StdEncoding.EncodeToString(make([]byte, maxConsoleBytes+1))},
	} {
		if _, err := bad.output(id); err == nil {
			t.Errorf("output(%.20q) accepted", bad.Output)
		}
	}
}

func TestCreditResponse(t *testing.T) {
	const id = "i-1234567890abcdef0"
	root := xml.Name{Space: ec2Namespace, Local: "DescribeInstanceCreditSpecificationsResponse"}
	item := func(id, credits string) string {
		return `<item><instanceId>` + id + `</instanceId><cpuCredits>` + credits + `</cpuCredits></item>`
	}
	doc := func(items, extra string) string {
		return `<DescribeInstanceCreditSpecificationsResponse xmlns="` + ec2Namespace + `"><requestId>r</requestId><instanceCreditSpecificationSet>` +
			items + `</instanceCreditSpecificationSet>` + extra + `</DescribeInstanceCreditSpecificationsResponse>`
	}
	tests := []struct {
		name string
		doc  string
		want string // empty if refused
	}{
		{"standard", doc(item(id, "standard"), ""), "standard"},
		{"unlimited", doc(item(id, "unlimited"), ""), "unlimited"},
		{"missing", doc("", ""), ""},
		{"duplicate item", doc(item(id, "standard")+item(id, "standard"), ""), ""},
		{"wrong ID", doc(item("i-0234567890abcdef0", "standard"), ""), ""},
		{"next token", doc(item(id, "standard"), "<nextToken>t</nextToken>"), ""},
		{"unknown CPU value", doc(item(id, "burst"), ""), ""},
		{"empty CPU value", doc(item(id, ""), ""), ""},
		{"duplicate CPU element", doc(`<item><instanceId>`+id+`</instanceId><cpuCredits>standard</cpuCredits><cpuCredits>unlimited</cpuCredits></item>`, ""), ""},
		{"wrong root", strings.Replace(doc(item(id, "standard"), ""), "DescribeInstanceCreditSpecificationsResponse", "DescribeInstancesResponse", 2), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out creditResponse
			got := ""
			if err := decodeXML([]byte(tt.doc), root, &out); err == nil {
				got, _ = out.credits(id)
			}
			if got != tt.want {
				t.Fatalf("credits = %q, want %q", got, tt.want)
			}
		})
	}
}

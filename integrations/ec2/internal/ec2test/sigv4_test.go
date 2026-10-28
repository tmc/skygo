package ec2test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestVerifySigV4KnownAnswer checks the verifier against the worked IAM
// ListUsers example in the AWS Signature Version 4 documentation.
func TestVerifySigV4KnownAnswer(t *testing.T) {
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	newReq := func(sig string) *http.Request {
		r := httptest.NewRequest("GET", "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08", nil)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		r.Header.Set("X-Amz-Date", "20150830T123600Z")
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature="+sig)
		return r
	}
	const sig = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
	if err := verifySigV4(newReq(sig), nil, creds, "us-east-1", "iam", now); err != nil {
		t.Fatalf("documented signature rejected: %v", err)
	}
	if err := verifySigV4(newReq("6"+sig[1:]), nil, creds, "us-east-1", "iam", now); err == nil {
		t.Fatal("altered signature accepted")
	}
}

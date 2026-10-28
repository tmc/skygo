package ec2api

import (
	"context"
	"crypto/x509"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestNewClientWithEndpoints(t *testing.T) {
	f := newFixture(t)
	roots := x509.NewCertPool()
	roots.AddCert(f.ec2.Certificate())
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return f.creds, nil })

	if _, err := NewClientWithEndpoints(testProfile(), creds, f.ec2.URL+"/", f.sts.URL+"/", nil); err == nil {
		t.Fatal("nil roots accepted")
	}
	if _, err := NewClientWithEndpoints(testProfile(), creds, "http://127.0.0.1/", f.sts.URL+"/", roots); err == nil {
		t.Fatal("http endpoint accepted")
	}

	c, err := NewClientWithEndpoints(testProfile(), creds, f.ec2.URL+"/", f.sts.URL+"/", roots)
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest(t)
	res, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Instances) != 1 || f.count("RunInstances") != 1 || f.count("GetCallerIdentity") != 1 {
		t.Fatalf("%d instances, %d creates, %d account checks", len(res.Instances), f.count("RunInstances"), f.count("GetCallerIdentity"))
	}

	// Roots without the fixture's certificate refuse the handshake: the
	// request is never answered.
	before := f.ec2Requests()
	c, err = NewClientWithEndpoints(testProfile(), creds, f.ec2.URL+"/", f.sts.URL+"/", x509.NewCertPool())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeOwned(context.Background(), req, res.Instances[0].ID)
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassTransport || e.StatusCode != 0 {
		t.Fatalf("untrusted endpoint: %v", err)
	}
	if f.ec2Requests() != before {
		t.Fatal("untrusted endpoint was answered")
	}
}

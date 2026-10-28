package ec2api

import (
	"crypto/x509"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// NewClientWithEndpoints returns a client like [NewClient] that sends EC2
// requests to ec2URL and STS requests to stsURL and trusts only the
// certificates in roots. Both URLs must be https URLs with no user
// information or query, and roots must not be nil.
//
// It exists so that a trusted local TLS endpoint, such as the fixture in
// package ec2test, can serve a real client. Nothing else changes: requests
// are signed for the profile's region and every other property of
// NewClient holds. The skygo-ec2 command never calls it and has no
// endpoint flag.
func NewClientWithEndpoints(p *Profile, creds aws.CredentialsProvider, ec2URL, stsURL string, roots *x509.CertPool) (*Client, error) {
	if roots == nil {
		return nil, errors.New("ec2api: custom endpoints need explicit trusted roots")
	}
	return newClient(p, creds, ec2URL, stsURL, roots)
}

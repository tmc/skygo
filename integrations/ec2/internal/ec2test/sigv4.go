package ec2test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// verifySigV4 verifies the AWS Signature Version 4 Authorization header of
// r, whose body is body, independently of the SDK signer. It is the
// verifier of the ec2api package's own fixture, checked there against the
// worked example in the AWS documentation.
func verifySigV4(r *http.Request, body []byte, creds aws.Credentials, region, service string, now time.Time) error {
	const alg = "AWS4-HMAC-SHA256"
	rest, ok := strings.CutPrefix(r.Header.Get("Authorization"), alg+" ")
	if !ok {
		return errors.New("no SigV4 authorization")
	}
	fields := strings.Split(rest, ", ")
	if len(fields) != 3 {
		return errors.New("authorization has wrong field count")
	}
	credential, ok1 := strings.CutPrefix(fields[0], "Credential=")
	signedList, ok2 := strings.CutPrefix(fields[1], "SignedHeaders=")
	signature, ok3 := strings.CutPrefix(fields[2], "Signature=")
	if !ok1 || !ok2 || !ok3 {
		return errors.New("authorization fields malformed")
	}
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[0] != creds.AccessKeyID || scope[2] != region || scope[3] != service || scope[4] != "aws4_request" {
		return errors.New("credential scope is wrong")
	}
	amzDate := r.Header.Get("X-Amz-Date")
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil || scope[1] != amzDate[:8] {
		return errors.New("x-amz-date is malformed or disagrees with the scope")
	}
	if d := now.Sub(t); d > 5*time.Minute || d < -5*time.Minute {
		return errors.New("x-amz-date is outside the allowed skew")
	}
	signed := strings.Split(signedList, ";")
	if !slices.IsSorted(signed) || len(slices.Compact(slices.Clone(signed))) != len(signed) ||
		!slices.Contains(signed, "host") || !slices.Contains(signed, "x-amz-date") {
		return errors.New("signed headers are incomplete or unsorted")
	}
	if creds.SessionToken != "" {
		if r.Header.Get("X-Amz-Security-Token") != creds.SessionToken || !slices.Contains(signed, "x-amz-security-token") {
			return errors.New("session token is missing or unsigned")
		}
	}
	var canon strings.Builder
	canon.WriteString(r.Method + "\n")
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canon.WriteString(path + "\n")
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	canon.WriteString(strings.ReplaceAll(query.Encode(), "+", "%20") + "\n")
	for _, h := range signed {
		var v string
		switch h {
		case "host":
			v = r.Host
		case "content-length":
			v = strconv.FormatInt(r.ContentLength, 10)
		default:
			v = strings.Join(r.Header.Values(h), ",")
		}
		canon.WriteString(h + ":" + strings.Join(strings.Fields(v), " ") + "\n")
	}
	canon.WriteString("\n" + signedList + "\n")
	sum := sha256.Sum256(body)
	canon.WriteString(hex.EncodeToString(sum[:]))

	reqHash := sha256.Sum256([]byte(canon.String()))
	toSign := alg + "\n" + amzDate + "\n" + strings.Join(scope[1:], "/") + "\n" + hex.EncodeToString(reqHash[:])
	key := []byte("AWS4" + creds.SecretAccessKey)
	for _, s := range []string{scope[1], region, service, "aws4_request"} {
		key = hmacSHA256(key, s)
	}
	want := hex.EncodeToString(hmacSHA256(key, toSign))
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return fmt.Errorf("signature does not match")
	}
	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

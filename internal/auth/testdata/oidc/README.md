# OIDC verification vectors

Public test data for the OIDC ID-token verifier. None of it is a credential.

- rfc7515-a2.json: RFC 7515 Appendix A.2 (RS256) compact JWS and the public
  members (kty, n, e) of its Figure A.2 key. The private members are omitted
  because parseJWKS refuses private key material.
- rfc7515-a3.json: RFC 7515 Appendix A.3 (ES256) compact JWS and the public
  members (kty, crv, x, y) of its key.
- rfc7638.json: RFC 7638 section 3.1 RSA key and its SHA-256 thumbprint.
- config.json: an OIDC configuration for the fixed test installation. Its
  issuer and URLs are reserved example names; carol has no credential-file
  principal in the tests.

The RFC payloads lack OIDC claims, so tests use them only for signature
verification. Claim and mapping tests mint tokens from keys generated with
crypto/rand at test time.

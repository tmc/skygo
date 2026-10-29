# OpenID Connect login

`skygo oidc login` runs an authorization-code flow with PKCE and saves the ID
token to a private file. Configure the server's issuer, audience and subject
mapping first; see [scoped authentication](authentication.md).

Register a public native client at the provider. Its callback is
`http://127.0.0.1:PORT/callback`; use `--redirect-port` when the provider needs a
fixed port. Supply each HTTPS endpoint explicitly:

```sh
mkdir -m 0700 ~/.skygo
skygo oidc login \
    --issuer https://issuer.example \
    --client-id skygo-cli \
    --authorization-endpoint https://issuer.example/authorize \
    --token-endpoint https://issuer.example/token \
    --output ~/.skygo/id.token
skygo --token-file ~/.skygo/id.token jobs list
```

The CLI opens the system browser. `--no-browser` prints the authorization URL
instead. That URL contains state, nonce and the PKCE challenge. It contains no
ID token, authorization code or verifier. Access and refresh tokens are discarded;
there is no automatic refresh or endpoint discovery. The token endpoint follows
no redirects and uses no proxy or cookie jar. `--ca-file` selects PEM roots in
place of system trust.

The CLI checks the issuer, audience, authorized party, nonce and expiry of the ID
token obtained directly over the configured TLS connection. The Skygo server
independently verifies its signature and current subject mapping and role. A
saved token does not establish server access; the `jobs list` command verifies
that separately. Tokens exceeding the CLI's 4096-byte bearer limit are refused.

The output directory must already exist, be owned by the caller and be private
(0700). Output traversal refuses symlinks. Files are published with mode0600;
existing output is refused unless `--replace` is explicit. After replacement,
the old token is not restored if a later directory sync or cleanup fails. An
error states whether publication occurred. An uncertain first Close retains its
original owner and any private temporary file; it is never retried or removed
blindly. Keep such files private when investigating the failure.

`--login-timeout` defaults to five minutes and permits at most fifteen. It starts
cancellation; the command still joins admitted callbacks and original operations.
A timeout is not evidence of closure. Output needs Linux or macOS descriptor
operations; other platforms refuse before opening a browser.

This command obtains a credential. It does not enable durable OIDC delegation,
workdir or rank authorization, shared-state HA, or a production identity-provider
deployment. Tests use an owned TLS issuer and a modeled browser redirect follower;
external provider and desktop browser acceptance are separate checks.

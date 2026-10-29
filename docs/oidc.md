# OpenID Connect authentication

A scoped server can accept ID tokens from one configured issuer. File credentials
continue to work. Initialize scoped authentication and add the principals first;
map issuer subjects to those existing principals in a private OIDC file.

Start the server with an operator-owned mapping file:

```sh
skygo server --auth-file ./auth.json --oidc-file ./oidc.json
```

The same server options are available in `skygo-kubernetes`. The EC2 VM lifecycle
command does not run an API server.

The OIDC file names exactly one `jwks_file` or `jwks_uri`. Network key retrieval
also requires the operator to allow that exact HTTPS URL:

```sh
skygo server --auth-file ./auth.json --oidc-file ./oidc.json \
  --oidc-jwks-url https://issuer.example/keys
```

No issuer discovery runs, and token contents cannot choose a URL. Redirects,
proxies and cookies are disabled for key retrieval. A relative `jwks_file` or
`ca_file` is resolved relative to the OIDC file. A configured CA replaces system
roots for this key source. Keep the credential and mapping files private and their
parent directories unwritable by other users.

For example, use the installation ID from scoped initialization and an existing
principal named `alice`:

```json
{
  "version": 1,
  "installation": "INSTALLATION_ID",
  "generation": 1,
  "issuer": "https://issuer.example",
  "audience": "skygo",
  "jwks_file": "keys.json",
  "clock_skew": "0s",
  "identities": [{"subject": "ISSUER_SUBJECT", "principal": "alice"}]
}
```

Supply a bounded ID token in a private file using the existing `--token-file`
option. Tokens must match the configured issuer, audience, subject mapping and
current key set. Expiry, mapping removal, principal changes and key changes are
rechecked for requests and active log streams. Do not put tokens on the command
line or include them in logs.

OIDC remains unavailable until the authentication loop loads its configuration
and keys. Ordinary issuer or mapping failures affect OIDC; scoped file credentials
remain available. Failed original file closure makes the Engine unavailable and
keeps its dependencies retained. Server shutdown joins the original authentication
loop and key fetch before closing those dependencies.

Browser sign-in, OIDC delegation to data or rank workers and durable antirollback
epochs have separate acceptance gates. This server configuration does not grant
those capabilities.

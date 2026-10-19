# Prior credential fixture

These are public test credentials, issued by the frozen pre-extension auth package whose canonical source is signed commit 4cf861bb60167d5045828987a3e5b39aa4951d4d. They belong only to the fixed nonproduction installation used by auth tests; never install this file as server authority.

prior-source.json pins the actual issuer source, fixture generator and output hashes. The old issuer generated the skygo_ token and full-byte digest together. Tests copy the JSON to a private temporary file and use its original timestamps with an injected clock, so ordinary calendar expiry does not invalidate the compatibility fixture. Mixed-issuance tests adjust only fixture validity timestamps and preserve the old credential digest.

The original issuance used archival development snapshot a6f9837e2a592ce706f054aceb75ac7d05939fc8; its five issuer source files are byte-identical to the canonical signed commit, which is reachable from the accepted S02 history. Use the canonical reference for clean-clone source verification. The archival reference records issuance history and is not required for reproduction.

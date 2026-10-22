// Package dashboard serves a local, read-only HTML view of one skygo endpoint.
//
// The dashboard is an ordinary API client. It calls only the generated read
// methods in Reader, and each page is a fresh RPC: the canonical server makes
// every authorization decision, so a page shows exactly what the caller's own
// credential may read. The credential is attached by the caller's HTTP client
// and never reaches this package, a page, a URL or browser storage.
//
// Listen accepts only the loopback literals 127.0.0.1 and [::1]. Serve prints
// the URL and a 128-bit one-time login code to its log writer. The code is
// accepted once, by the dashboard's script, for ten minutes and at most five
// attempts. It mints one in-memory session with two secrets: an HttpOnly,
// SameSite=Strict cookie, and a proof that the script keeps in the origin's
// sessionStorage and sends in the X-Skygo-Session header. Browsers send the
// cookie to every port of the host but never send the header on their own,
// so data requires both; a navigation with the cookie alone gets a static
// shell page whose script refetches the view with the proof.
//
// Every request must carry the exact bound Host, and every POST a same-origin
// Origin. Only GET and POST are served. Responses allow only same-origin
// scripts, styles and fetches, and forbid framing, sniffing and caching.
//
// The dashboard has no mutation, cancellation or artifact download path.
// Stopping it closes open log streams and never affects the job.
package dashboard

# Local dashboard

`skygo dashboard` serves a read-only web view of one endpoint on this machine.
It shows jobs, attempts, logs, targets and server capabilities using the same
read RPCs and server checks as the CLI. It cannot submit, cancel, prune, retry,
upload or download anything, and stopping it never changes a job.

```sh
skygo --token-file ~/.config/skygo/token dashboard
```

The command prints two lines to standard error:

```
dashboard: http://127.0.0.1:53412/
login code: 3f0c...
```

Open the URL and enter the code; the dashboard needs JavaScript. The code is
128 random bits, works for one login, expires after ten minutes and is spent
by five wrong guesses. Restart the command for a new code.

One browser tab holds the session at a time. Logging in from a second
browser, or using the session from a new tab, is not possible: links opened
in the same tab keep working. The session ends after 30 idle minutes, after
eight hours, at log out, or when the command stops. It lives only in memory
on the dashboard side.

## Listener

`--listen` takes `127.0.0.1:PORT` or `[::1]:PORT`; the default is
`127.0.0.1:0`, a free port. Host names such as `localhost`, wildcard
addresses, other loopback addresses and zoned addresses are refused before
binding. The dashboard answers only requests whose `Host` is exactly the bound
address, and accepts a POST only from its own origin. There is no TLS: the
dashboard is for a browser on the same machine. Other local users can reach
the port, which is why a login code is required.

Browsers send cookies to every port of a host, so another web server on the
same loopback address that the browser visits can receive the session cookie
(`skygo_dashboard_PORT`). The cookie alone authorizes nothing: every request
for data, logs or log out must also carry the session proof in the
`X-Skygo-Session` header. Only the dashboard's own script sends that header.
It keeps the proof in this tab's `sessionStorage`, which belongs to the
dashboard's origin (including its port), and it is deleted at log out.

Such a server can also set cookies for the dashboard. A same-named cookie
with another path is ignored, but over plain HTTP nothing stops a response
from another port from replacing the session cookie itself. The tab then
shows "session cookie missing" and keeps its proof; the session cannot be
used again until it expires or the dashboard restarts.

## Credential

The credential comes from the usual sources (`--token-file`, `SKYGO_TOKEN` or
the deprecated `--token`). It stays in the command's HTTP client, which sends
it only to the endpoint's own scheme and host and never follows redirects. It
is never put in a URL, page, cookie or browser storage, and the browser cannot
choose another endpoint or principal. Pages show what that credential may read: a
job the credential cannot see is "not found", exactly as a missing job is. If
the server rejects the credential, for example after revocation, pages show
"credential rejected" and the dashboard does not retry.

## Pages

- `/jobs` lists jobs, 50 per page.
- `/jobs/ID` shows a job, its attempts and its task, and whether the server
  has bounded retention enabled (enabled, disabled, or why that is
  unknown). Environment variables show names only, never values. A
  result shows its state and attempt; the dashboard offers no result
  download.
- `/jobs/ID/logs` shows up to 64 KiB of output per page, with a link to the
  next page. Control characters are shown as visible symbols and invalid UTF-8
  as U+FFFD.
- `/jobs/ID/logs.txt` returns raw output as `text/plain`, at most 1 MiB per
  response. Add `follow=1` to stream until the job's output completes, for at
  most ten minutes; four followed streams may be open at once.
- `/targets` and `/info` show targets and capabilities as `skygo targets list`
  and `skygo info` do.

Log pages accept `attempt` (1 to 10), `offset` and `cursor` with the meanings
of `skygo jobs logs`, and select retained or legacy logs the same way.
Output that is not known to be whole ends with marker lines:
`-- output truncated: retained output limit reached --` when the server
truncated it, and `-- output incomplete: the job may write more --`,
`-- response limit reached --` or a line such as
`-- stream ended: unauthenticated --` when it is incomplete, capped or ended
early. Complete output has no marker.

Responses forbid caching, framing and referrers, and allow only the
dashboard's own script file: no inline or third-party script. That script
fetches each view, with the session proof, into a page that holds no data;
`logs.txt` is shown as plain text in the page. Only GET and POST are served.
Only POST requests (log in, log out) may carry a body, of at most 1 KiB, which
must arrive within ten seconds; other requests with a body are refused, as is
any request with an `Expect` header. The dashboard serves at most 16 requests
at once, beyond which it answers `503`; a POST body is read before a request
counts toward that limit.

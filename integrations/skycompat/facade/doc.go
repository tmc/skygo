// Package facade serves a bounded SkyPilot API67 health and validation subset.
// It forwards each caller's Authorization header to a configured canonical
// Skygo endpoint. It does not submit work or implement asynchronous requests.
//
// The embedding HTTP server must set ReadHeaderTimeout, MaxHeaderBytes and its
// connection policy. The 32-operation cap covers supported operations; every
// rejection path also establishes body/write deadlines. These require a writer that
// supports http.ResponseController deadlines, such as net/http's server writer.
// The canonical connection requires trusted transport: HTTPS or an explicitly
// trusted private HTTP network. No cookies, redirects or proxy environment are
// used. API67 denotes this selected wire profile, not the full upstream API.
package facade

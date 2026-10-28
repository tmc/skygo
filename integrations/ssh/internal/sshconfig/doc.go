// Package sshconfig resolves an OpenSSH host alias to the address, user and
// identity settings skygo-ssh connects with.
//
// Resolve runs the local `ssh -G -- ALIAS`, which evaluates the user's
// OpenSSH configuration without connecting, and accepts only the settings
// skygo-ssh can honor with a direct connection. Proxies, agent forwarding,
// port forwarding and remote or local commands are refused rather than
// silently bypassed.
package sshconfig

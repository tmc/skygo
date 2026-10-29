#!/bin/sh
# HOLD. This native installation check refuses unconditionally, before any
# network access, build, remote command or local effect. It reads no
# argument or environment variable. The intended cases are kept as a
# non-executable recipe in README.md, "Native installation check (HOLD)".
printf '%s\n' \
	"native-installation: HOLD: refusing before any network access or effect." \
	"native-installation: no safe agent stop: agent.pid records only a process ID; the bootstrap" \
	"  records no boot ID, start ticks, UID, executable, working directory or configuration for" \
	"  the started agent and has no pidfd-bound stop that waits for that exact process to exit." \
	"native-installation: no admitted binary: skygo-ssh, its agent and the operator skygo must" \
	"  be bound by SHA-256 to a separately admitted real Git checkout, not found on PATH." \
	"native-installation: no root admission for native runtime. Native work is NOT_RUN." >&2
exit 2

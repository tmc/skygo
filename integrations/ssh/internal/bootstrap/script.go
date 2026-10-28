package bootstrap

import (
	"errors"
	"strings"
)

// script is the only remote program. It runs as `sh -c SCRIPT
// skygo-bootstrap MODE COMMIT DIGEST [ARGS]` with strictly validated
// operands, which it validates again. It writes only below
// ~/skygo-test/COMMIT/DIGEST, creating ~/skygo-test and the commit
// directory if needed, and contains no single quote, so it can be quoted
// for the remote login shell without escapes.
//
// Modes:
//
//	status                        print arch, each file's SHA-256 and the agent state
//	put NAME SHA256               store standard input as NAME if it is absent
//	start PORT TARGET RESTART     start the agent unless one is recorded
//	log                           print the end of agent.log
//
// The first start records PORT and TARGET in agent.config, exactly
// "port PORT\ntarget TARGET\n", mode 0400, linked into place only if
// absent and never rewritten. Every later start requires the record to be
// a regular file holding exactly that text. If it holds a well-formed
// record for another port or target, start changes nothing and prints
// "agent configuration-mismatch"; any other record is refused. A running
// agent without a record is refused, since its configuration is unknown;
// a stopped one may be restarted with a new record.
const script = `set -eu
umask 077
fail() { printf "skygo-bootstrap: %s\n" "$*" >&2; exit 1; }
hex() { case $1 in "" | *[!0-9a-f]*) return 1 ;; esac; }
sum() {
	if command -v sha256sum >/dev/null 2>&1; then set -- "$(sha256sum <"$1")"
	else set -- "$(shasum -a 256 <"$1")"; fi
	printf "%s\n" "${1%% *}"
}
portok() { case $1 in "" | 0* | *[!0-9]*) return 1 ;; esac; [ ${#1} -le 5 ] && [ "$1" -ge 1024 ] && [ "$1" -le 65535 ]; }
targetok() { case $1 in "" | *[!a-z0-9.-]*) return 1 ;; esac; [ ${#1} -le 128 ]; }
mode=${1-} commit=${2-} digest=${3-}
case ${#commit} in 40 | 64) ;; *) fail "invalid commit" ;; esac
hex "$commit" || fail "invalid commit"
[ ${#digest} -eq 64 ] && hex "$digest" || fail "invalid digest"
case $HOME in /*) ;; *) fail "HOME is not an absolute path" ;; esac
root=${HOME%/}/skygo-test
dir=$root/$commit/$digest
bin=$dir/skygo-ssh
pidfile=$dir/agent.pid
cfg=$dir/agent.config
want= seen= canon=
dirs() {
	for d in "$root" "$root/$commit" "$dir"; do
		if [ -L "$d" ]; then fail "$d is a symlink"; fi
		if [ -e "$d" ] && [ ! -d "$d" ]; then fail "$d is not a directory"; fi
		if [ "${1-}" = create ] && [ ! -d "$d" ]; then mkdir -m 700 "$d"; fi
	done
}
state() {
	if [ -L "$pidfile" ]; then echo unsafe; return; fi
	if [ ! -e "$pidfile" ]; then echo none; return; fi
	pid=$(head -c 32 "$pidfile")
	case $pid in "" | *[!0-9]*) echo unknown; return ;; esac
	if kill -0 "$pid" 2>/dev/null; then :; else echo "stopped $pid"; return; fi
	exe=$(readlink "/proc/$pid/exe" 2>/dev/null) || exe=
	real=$(cd "$dir" 2>/dev/null && pwd -P)/skygo-ssh
	if [ "$exe" = "$real" ]; then echo "running $pid"; else echo "unknown $pid"; fi
}
temp() { t=$dir/.config-$1-$$; (set -C; : >"$t") 2>/dev/null || fail "$t exists"; }
clean() { for t in "$want" "$seen" "$canon"; do if [ -n "$t" ]; then rm -f "$t"; fi; done; }
# config compares agent.config with the record in $want and prints same,
# other for a well-formed record of another configuration, none or unsafe.
# It reads at most 256 bytes, more than any record holds, so a copy equal
# to a record means the file is exactly that record.
config() {
	if [ -L "$cfg" ]; then echo unsafe; return; fi
	if [ ! -e "$cfg" ]; then echo none; return; fi
	if [ ! -f "$cfg" ]; then echo unsafe; return; fi
	head -c 256 "$cfg" >"$seen" 2>/dev/null || { echo unsafe; return; }
	if [ "$(sum "$seen")" = "$(sum "$want")" ]; then echo same; return; fi
	{ IFS= read -r l1 && IFS= read -r l2; } <"$seen" || { echo unsafe; return; }
	p=${l1#"port "} g=${l2#"target "}
	if [ "$p" = "$l1" ] || [ "$g" = "$l2" ] || ! portok "$p" || ! targetok "$g"; then echo unsafe; return; fi
	printf "port %s\ntarget %s\n" "$p" "$g" >"$canon"
	if [ "$(sum "$seen")" = "$(sum "$canon")" ]; then echo other; else echo unsafe; fi
}
mismatch() { echo "agent configuration-mismatch"; exit 0; }
unverifiable() { fail "agent configuration $cfg is unverifiable; inspect it, stop any agent process, then remove it"; }
# record links $want into place as agent.config. If another start created
# the record first, record accepts it only if it is the same.
record() {
	chmod 400 "$want"
	if ln "$want" "$cfg" 2>/dev/null; then return; fi
	case $(config) in
	same) ;;
	other) mismatch ;;
	*) unverifiable ;;
	esac
}
case $mode in
status)
	dirs
	printf "arch %s\n" "$(uname -m)"
	for f in skygo-ssh cert.pem key.pem token; do
		p=$dir/$f
		if [ -L "$p" ]; then echo "file $f unsafe"
		elif [ -f "$p" ]; then echo "file $f $(sum "$p")"
		elif [ -e "$p" ]; then echo "file $f unsafe"
		else echo "file $f missing"; fi
	done
	echo "agent $(state)"
	;;
put)
	name=${4-} want=${5-}
	case $name in skygo-ssh | cert.pem | key.pem | token) ;; *) fail "invalid file name" ;; esac
	[ ${#want} -eq 64 ] && hex "$want" || fail "invalid checksum"
	if [ "$name" = skygo-ssh ] && [ "$want" != "$digest" ]; then fail "binary checksum is not the digest"; fi
	dirs create
	p=$dir/$name
	if [ -e "$p" ] || [ -L "$p" ]; then fail "$name already exists"; fi
	tmp=$dir/.put-$name-$$
	(set -C; : >"$tmp") 2>/dev/null || fail "$tmp exists"
	head -c 268435456 >"$tmp"
	if [ "$(sum "$tmp")" != "$want" ]; then rm -f "$tmp"; fail "$name checksum differs after transfer"; fi
	if [ "$name" = skygo-ssh ]; then chmod 500 "$tmp"; else chmod 400 "$tmp"; fi
	if ln "$tmp" "$p" 2>/dev/null; then rm -f "$tmp"; else rm -f "$tmp"; fail "$name appeared during transfer"; fi
	echo "stored $name"
	;;
start)
	port=${4-} target=${5-} restart=${6-}
	portok "$port" || fail "invalid port"
	targetok "$target" || fail "invalid target name"
	case $restart in 0 | 1) ;; *) fail "invalid restart flag" ;; esac
	dirs
	if [ -L "$bin" ] || [ ! -f "$bin" ]; then fail "agent binary is missing"; fi
	[ "$(sum "$bin")" = "$digest" ] || fail "agent binary differs from its digest; refusing to run it"
	s=$(state)
	case $s in
	none | running* | stopped*) ;;
	*) fail "agent state $s; inspect $dir/agent.log, stop any agent process, then remove $pidfile" ;;
	esac
	trap clean EXIT
	temp want; want=$t
	temp seen; seen=$t
	temp canon; canon=$t
	printf "port %s\ntarget %s\n" "$port" "$target" >"$want"
	c=$(config)
	case $c in
	none | same) ;;
	other) mismatch ;;
	*) unverifiable ;;
	esac
	case $s in
	running*)
		[ "$c" = same ] || fail "agent $s has no configuration record, so its port and target are unverifiable; stop it, then pass --restart-stopped"
		echo "agent $s"
		exit 0
		;;
	stopped*) [ "$restart" = 1 ] || fail "agent $s; pass --restart-stopped to start a new process" ;;
	esac
	if [ "$c" = none ]; then record; fi
	case $s in stopped*) mv "$pidfile" "$pidfile.stopped" ;; esac
	(set -C; : >"$pidfile") 2>/dev/null || fail "another start holds $pidfile"
	cd "$dir"
	if command -v setsid >/dev/null 2>&1; then
		setsid nohup "$bin" agent --dir "$dir" --listen "127.0.0.1:$port" --target "$target" </dev/null >>agent.log 2>&1 &
	else
		nohup "$bin" agent --dir "$dir" --listen "127.0.0.1:$port" --target "$target" </dev/null >>agent.log 2>&1 &
	fi
	echo "$!" >"$pidfile"
	echo "agent started $!"
	;;
log)
	dirs
	if [ -f "$dir/agent.log" ] && [ ! -L "$dir/agent.log" ]; then tail -c 4096 "$dir/agent.log"; fi
	;;
*) fail "invalid mode" ;;
esac
`

// command returns the remote command line running script with operands.
// Every operand must be a safe word; each is quoted regardless, because
// the remote login shell interprets the line.
func command(operands ...string) (string, error) {
	var b strings.Builder
	b.WriteString("sh -c ")
	b.WriteString(quote(script))
	b.WriteString(" skygo-bootstrap")
	for _, s := range operands {
		if !safeWord(s) {
			return "", errors.New("bootstrap: unsafe operand")
		}
		b.WriteString(" ")
		b.WriteString(quote(s))
	}
	return b.String(), nil
}

// quote quotes s for a POSIX shell.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func safeWord(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	return strings.Trim(s, "abcdefghijklmnopqrstuvwxyz0123456789.-") == ""
}

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
//	status PORT TARGET [DATA AUTH ARTIFACTS]
//	                              print arch, each file's SHA-256, the agent state,
//	                              and the record's form and whether it is the given one
//	put NAME SHA256               store standard input as NAME if it is absent
//	start PORT TARGET RESTART [DATA AUTH ARTIFACTS]
//	                              start the agent unless one is recorded
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
//
// Status and start read the record with the same parser, which accepts
// only those exact forms and creates nothing. Status prints "config none",
// "config unsafe", or "config FORM same" or "config FORM other" for a
// record of FORM legacy or installation; it never prints the record.
//
// A start naming an installation, by the clean absolute paths DATA, AUTH
// and ARTIFACTS, appends "data DATA\nauth AUTH\nartifacts ARTIFACTS\n" to
// the record, so a record of either form is "another configuration" for a
// start of the other. Before anything else it requires DATA and ARTIFACTS
// to be existing directories, DATA/jobs.db and AUTH existing regular
// files, none of them symlinks, and the agent directory to hold no token;
// it never creates or changes them. The agent is then started with
// --data-dir, --auth-file and --artifact-dir.
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
pathok() {
	case $1 in /?*) ;; *) return 1 ;; esac
	case $1 in */ | *//* | */./* | */../* | */. | */.. | *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-]*) return 1 ;; esac
	[ ${#1} -le 200 ]
}
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
want=
nl="
"
readok=" skygo-read-ok" readfailed=" skygo-read-failed"
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
# desired sets port, target, data, auth and artifacts from the operands
# PORT TARGET [DATA AUTH ARTIFACTS], which must be valid.
desired() {
	case $# in 2 | 5) ;; *) fail "invalid operands" ;; esac
	port=$1 target=$2 data=${3-} auth=${4-} artifacts=${5-}
	portok "$port" || fail "invalid port"
	targetok "$target" || fail "invalid target name"
	if [ $# -eq 5 ]; then
		pathok "$data" && pathok "$auth" && pathok "$artifacts" || fail "invalid installation path"
	fi
}
# line moves the first line of $rec, without its newline, to $l. It
# fails if $rec holds no newline.
line() {
	case $rec in *"$nl"*) ;; *) return 1 ;; esac
	l=${rec%%"$nl"*}
	rec=${rec#*"$nl"}
}
# field moves the first line of $rec, which must be "$1 VALUE", to $v.
field() { line || return 1; v=${l#"$1 "}; [ "$v" != "$l" ]; }
# parse is the one reader of agent.config. It sets form to none, unsafe,
# legacy or installation, and for a record sets rport, rtarget, rdata,
# rauth and rartifacts. A record is a regular file of at most 1024 bytes
# holding exactly "port PORT\ntarget TARGET\n", for legacy, or that
# followed by "data DATA\nauth AUTH\nartifacts ARTIFACTS\n", for an
# installation, each value valid as the operand of that name. parse reads
# the file once, through one pipe, and creates nothing. head's bytes are
# followed by a trailer, the last bytes in the pipe, saying whether head
# succeeded; tr then maps NUL, which command substitution would drop, to
# a byte no field accepts, and a final x keeps trailing newlines. parse
# requires tr to succeed and the success trailer, removes exactly that
# trailer, and reads a 1025th byte to refuse a longer file. A failed read
# or transform is unsafe, whatever bytes it delivered.
parse() {
	form=unsafe rport= rtarget= rdata= rauth= rartifacts=
	if [ -L "$cfg" ]; then return 0; fi
	if [ ! -e "$cfg" ]; then form=none; return 0; fi
	if [ ! -f "$cfg" ]; then return 0; fi
	rec=$({
		if head -c 1025 "$cfg" 2>/dev/null; then printf "%s" "$readok"; else printf "%s" "$readfailed"; fi
	} | tr "\000" "#" && echo x) || return 0
	rec=${rec%x}
	case $rec in *"$readok") rec=${rec%"$readok"} ;; *) return 0 ;; esac
	[ ${#rec} -le 1024 ] || return 0
	field port && portok "$v" || return 0
	rport=$v
	field target && targetok "$v" || return 0
	rtarget=$v
	if [ -z "$rec" ]; then form=legacy; return 0; fi
	field data && pathok "$v" || return 0
	rdata=$v
	field auth && pathok "$v" || return 0
	rauth=$v
	field artifacts && pathok "$v" || return 0
	rartifacts=$v
	[ -z "$rec" ] || return 0
	form=installation
}
# same reports whether the record parse read is the desired one.
same() {
	[ "$rport" = "$port" ] && [ "$rtarget" = "$target" ] && [ "$rdata" = "$data" ] &&
		[ "$rauth" = "$auth" ] && [ "$rartifacts" = "$artifacts" ]
}
# config prints none, unsafe, same for the desired record, or other for a
# record of another configuration, of either form.
config() {
	parse
	case $form in
	none | unsafe) echo "$form" ;;
	*) if same; then echo same; else echo other; fi ;;
	esac
}
mismatch() { echo "agent configuration-mismatch"; exit 0; }
unverifiable() { fail "agent configuration $cfg is unverifiable; inspect it, stop any agent process, then remove it"; }
clean() { if [ -n "$want" ]; then rm -f "$want"; fi; }
# record writes the desired record to a new file and links it into place
# as agent.config. If another start created the record first, record
# accepts it only if it is the same.
record() {
	t=$dir/.config-want-$$
	(set -C; : >"$t") 2>/dev/null || fail "$t exists"
	want=$t
	trap clean EXIT
	printf "port %s\ntarget %s\n" "$port" "$target" >"$want"
	if [ -n "$data" ]; then printf "data %s\nauth %s\nartifacts %s\n" "$data" "$auth" "$artifacts" >>"$want"; fi
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
	shift 3
	desired "$@"
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
	parse
	case $form in
	none | unsafe) echo "config $form" ;;
	*) if same; then echo "config $form same"; else echo "config $form other"; fi ;;
	esac
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
	restart=${6-}
	case $# in
	6) desired "$4" "$5" ;;
	9) desired "$4" "$5" "$7" "$8" "$9" ;;
	*) fail "invalid operands" ;;
	esac
	case $restart in 0 | 1) ;; *) fail "invalid restart flag" ;; esac
	dirs
	if [ -L "$bin" ] || [ ! -f "$bin" ]; then fail "agent binary is missing"; fi
	[ "$(sum "$bin")" = "$digest" ] || fail "agent binary differs from its digest; refusing to run it"
	if [ -n "$data" ]; then
		if [ -e "$dir/token" ] || [ -L "$dir/token" ]; then fail "$dir holds a legacy token; refusing to serve an installation from it"; fi
		for d in "$data" "$artifacts"; do
			if [ -L "$d" ] || [ ! -d "$d" ]; then fail "$d is not an existing directory; initialize the installation on the host first"; fi
		done
		for p in "$data/jobs.db" "$auth"; do
			if [ -L "$p" ] || [ ! -f "$p" ]; then fail "$p is not an existing regular file; initialize the installation on the host first"; fi
		done
	fi
	s=$(state)
	case $s in
	none | running* | stopped*) ;;
	*) fail "agent state $s; inspect $dir/agent.log, stop any agent process, then remove $pidfile" ;;
	esac
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
	set -- agent --dir "$dir" --listen "127.0.0.1:$port" --target "$target"
	if [ -n "$data" ]; then set -- "$@" --data-dir "$data" --auth-file "$auth" --artifact-dir "$artifacts"; fi
	if command -v setsid >/dev/null 2>&1; then
		setsid nohup "$bin" "$@" </dev/null >>agent.log 2>&1 &
	else
		nohup "$bin" "$@" </dev/null >>agent.log 2>&1 &
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
// Every operand must be a safe word or a valid installation path; each is
// quoted regardless, because the remote login shell interprets the line.
func command(operands ...string) (string, error) {
	var b strings.Builder
	b.WriteString("sh -c ")
	b.WriteString(quote(script))
	b.WriteString(" skygo-bootstrap")
	for _, s := range operands {
		if !safeWord(s) && !ValidPath(s) {
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

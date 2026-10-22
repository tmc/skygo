#!/bin/bash
# build.sh ARCH OUT cross-builds the static Linux skygo-data helper into a new
# image context and records how it was built. See README.md.
set -eu -o pipefail
unset CDPATH

die() {
	echo "build.sh: $*" >&2
	exit 1
}
[ $# -eq 2 ] || { echo "usage: build.sh amd64|arm64 OUT" >&2; exit 2; }
arch=$1
# The baseline instruction level, so the helper runs on every node of ARCH.
case $arch in
amd64) level=GOAMD64=v1 ;;
arm64) level=GOARM64=v8.0 ;;
*) die "unsupported architecture: $arch" ;;
esac

here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "$here/../../.." && pwd -P)
[ "$(sed -n 1p "$root/go.mod")" = "module github.com/tmc/skygo" ] || die "$root is not the skygo module"
[ -f "$root/cmd/skygo-data/main.go" ] || die "$root has no cmd/skygo-data"

GO=${GO:-go}
gocmd=$(type -P "$GO") || die "go command not found: $GO"
case $gocmd in
/*) ;;
*) gocmd=$(cd "$(dirname "$gocmd")" && pwd -P)/$(basename "$gocmd") ;;
esac
# GOENV=off: a go env file cannot change the build. GOROOT is the Go
# command's own, not the caller's.
export GOENV=off GOTOOLCHAIN=local GOWORK=off GOPROXY=off GOSUMDB=off GOFLAGS=-mod=readonly
export CGO_ENABLED=0 GOOS=linux GOARCH=$arch GOAMD64=v1 GOARM64=v8.0 GOEXPERIMENT='' GOFIPS140=off
unset GOCACHEPROG
goroot=$(env -u GOROOT "$gocmd" env GOROOT) || die "go env GOROOT failed"
export GOROOT=$goroot
want=go$(awk '$1 == "go" { print $2; exit }' "$root/go.mod")
have=$("$gocmd" env GOVERSION) || die "go env GOVERSION failed"
[ "$have" = "$want" ] || die "go version $have, want $want from go.mod"

parent=$(cd "$(dirname "$2")" && pwd -P) || die "output parent does not exist: $(dirname "$2")"
out=$parent/$(basename "$2")
# Receipt and source records are one value per line.
case $gocmd$out in
*$'\n'*) die "go command or output path contains a newline" ;;
esac
# Compare directories, not names, so case or links cannot hide the tree.
d=$parent
while :; do
	[ "$d" -ef "$root" ] && die "output must be outside the source tree: $out"
	[ "$d" = / ] && break
	d=$(dirname "$d")
done
mkdir "$out" || die "output exists or cannot be created: $out"
mkdir "$out/context"

sha() {
	if command -v sha256sum >/dev/null; then
		sha256sum "$1" | cut -c1-64
	else
		shasum -a 256 "$1" | cut -c1-64
	fi
}

# The exact main-module files compiled into the helper, with go.mod and go.sum.
cd "$root"
{
	echo go.mod
	echo go.sum
	# shellcheck disable=SC2016 # A Go template, not shell expansion.
	"$gocmd" list -deps -f '{{if and .Module .Module.Main}}{{$d := .Dir}}{{range .GoFiles}}{{$d}}/{{.}}
{{end}}{{range .EmbedFiles}}{{$d}}/{{.}}
{{end}}{{end}}' ./cmd/skygo-data | sed "/^$/d; s|^$root/||"
} | LC_ALL=C sort | while read -r f; do
	h=$(sha "$f")
	printf '%s  %s\n' "$h" "$f"
done >"$out/source.txt"

flags=(-trimpath -buildvcs=false)
"$gocmd" build "${flags[@]}" -o "$out/context/skygo-data" ./cmd/skygo-data
chmod 0555 "$out/context/skygo-data"
"$gocmd" version -m "$out/context/skygo-data" >"$out/buildinfo.txt"
for setting in CGO_ENABLED=0 GOOS=linux GOARCH=$arch $level; do
	grep -q "^	build	$setting\$" "$out/buildinfo.txt" || die "helper build info lacks $setting"
done
! grep -q "^	build	GOEXPERIMENT=" "$out/buildinfo.txt" || die "helper build info has a GOEXPERIMENT"
cp "$here/Dockerfile" "$out/context/Dockerfile"
chmod 0444 "$out/context/Dockerfile"

# Compute every value before writing, so any failure stops the script;
# receipt.txt is written last and exists only after a complete build.
files=$(wc -l <"$out/source.txt" | tr -d ' ')
source=$(sha "$out/source.txt")
gosha=$(sha "$gocmd")
host=$(uname -s)/$(uname -m)
modules=$(grep -c "^	dep	" "$out/buildinfo.txt" || true)
bytes=$(wc -c <"$out/context/skygo-data" | tr -d ' ')
helper=$(sha "$out/context/skygo-data")
dockerfile=$(sha "$out/context/Dockerfile")
context=$(printf %q "$out/context")

cat >"$out/commands.txt" <<EOF
# Replace REPO with a lowercase repository with an explicit registry host and
# no tag (docker.io/NAME/skygo-data, registry.example:5000/skygo-data), then
# record the reported values in receipt.txt. build.sh ran none of these.
docker build --platform linux/$arch --build-arg HELPER_SHA256=$helper -t REPO:$helper $context
docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}' REPO:$helper
docker push REPO:$helper
docker buildx imagetools inspect REPO:$helper
# Target configuration:
#   helper_image: REPO@<digest reported by imagetools inspect>
#   helper_sha256: $helper
EOF

cat >"$out/receipt.txt.tmp" <<EOF
format skygo-helper-build-v1
source_module github.com/tmc/skygo
source_files $files
source_sha256 $source
go_command $gocmd
go_command_sha256 $gosha
goroot $goroot
go_version $have
host $host
goos linux
goarch $arch
goarch_level ${level#*=}
goexperiment none
gofips140 off
cgo_enabled 0
gotoolchain local
goflags -mod=readonly
build_flags ${flags[*]}
modules $modules
helper_path /skygo-data
helper_mode 0555
image_user 65532:65532
helper_bytes $bytes
helper_sha256 $helper
dockerfile_sha256 $dockerfile
image_id UNRUN
image_platform UNRUN
image_digest UNRUN
EOF
mv "$out/receipt.txt.tmp" "$out/receipt.txt"
echo "$out"

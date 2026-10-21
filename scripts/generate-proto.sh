#!/bin/sh
# Run from any directory. Tool modules are pinned separately from runtime deps.
set -eu
cd "$(dirname "$0")/.."
proto_root=$(pwd)
proto_bin="$proto_root/bin/proto"
mkdir -p "$proto_bin"

install_tool() {
  tool_name=$1
  tool_version=$2
  tool_module=$3
  if [ ! -x "$proto_bin/$tool_name" ] || [ "$("$proto_bin/$tool_name" --version)" != "$tool_version" ]; then
    GOBIN="$proto_bin" go install "$tool_module"
  fi
  test "$("$proto_bin/$tool_name" --version)" = "$tool_version"
}

install_tool buf '1.74.0' github.com/bufbuild/buf/cmd/buf@v1.74.0
install_tool protoc-gen-go 'protoc-gen-go v1.36.12' google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
install_tool protoc-gen-go-grpc 'protoc-gen-go-grpc 1.6.2' google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

"$proto_bin/buf" format --diff --exit-code
"$proto_bin/buf" lint
case "${1:-}" in
  '') "$proto_bin/buf" generate ;;
  --check)
    proto_tmp=$(mktemp -d)
    trap 'rm -rf "$proto_tmp"' EXIT HUP INT TERM
    "$proto_bin/buf" generate --output "$proto_tmp"
    diff -ru gen "$proto_tmp/gen"
    ;;
  *) echo 'Usage: sh scripts/generate-proto.sh [--check]' >&2; exit 2 ;;
esac

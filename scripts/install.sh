#!/bin/sh
# Install the latest pgsync release binary.
#
#   curl -fsSL https://raw.githubusercontent.com/pgrundev/pgsync/main/scripts/install.sh | sh
#
# Env: PGSYNC_VERSION (default: latest), PGSYNC_INSTALL_DIR (default: ~/.local/bin).
# Verifies the archive against the release's checksums.txt before installing.
set -eu
repo="pgrundev/pgsync"
dir="${PGSYNC_INSTALL_DIR:-$HOME/.local/bin}"
case "$(uname -s)" in Linux) os=linux ;; Darwin) os=darwin ;; *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;; esac
case "$(uname -m)" in x86_64|amd64) arch=amd64 ;; arm64|aarch64) arch=arm64 ;; *) echo "unsupported CPU: $(uname -m)" >&2; exit 1 ;; esac
tag="${PGSYNC_VERSION:-}"
if [ -z "$tag" ]; then
  # Includes pre-releases (all 0.x releases are marked pre-release).
  tag=$(curl -fsSL "https://api.github.com/repos/$repo/releases" | grep -m1 '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
fi
[ -n "$tag" ] || { echo "could not determine the release tag" >&2; exit 1; }
v="${tag#v}"; name="pgsync_${v}_${os}_${arch}"
base="https://github.com/$repo/releases/download/$tag"
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
echo "downloading $name ($tag)"
if ! curl -fsSL -o "$tmp/$name.tar.gz" "$base/$name.tar.gz"; then
  echo "no prebuilt binary for ${os}_${arch} in $tag; use: brew install pgrundev/tap/pgsync  or  cargo install pgsync" >&2
  exit 1
fi
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt"
expected=$(grep " $name.tar.gz\$" "$tmp/checksums.txt" | cut -d' ' -f1)
actual=$( (command -v sha256sum >/dev/null && sha256sum "$tmp/$name.tar.gz" || shasum -a 256 "$tmp/$name.tar.gz") | cut -d' ' -f1)
[ -n "$expected" ] && [ "$expected" = "$actual" ] || { echo "checksum mismatch for $name.tar.gz" >&2; exit 1; }
tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$dir"
install -m 0755 "$tmp/$name/pgsync" "$dir/pgsync"
echo "installed $("$dir/pgsync" --version) to $dir/pgsync"
case ":$PATH:" in *":$dir:"*) ;; *) echo "add $dir to PATH" ;; esac

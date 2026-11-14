#!/usr/bin/env bash
# Builds a Docker image of Gratis at any git ref and prints its tag, gratis:<short sha>.
# Usage: bench/build-image.sh REF
set -euo pipefail
ref=${1:?usage: build-image.sh REF}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
sha=$(git -C "$repo" rev-parse --short=7 "$ref^{commit}")
work=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$work/tree" 2>/dev/null; rm -rf "$work"' EXIT
git -C "$repo" worktree add --detach --quiet "$work/tree" "$sha"
docker build --quiet --tag "gratis:$sha" --file "$here/Dockerfile" "$work/tree" >/dev/null
echo "gratis:$sha"

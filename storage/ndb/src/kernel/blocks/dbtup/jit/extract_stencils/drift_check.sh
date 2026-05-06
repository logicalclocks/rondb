#!/usr/bin/env bash
#
# Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# RONDB-1056 Phase 2 — generated-header drift check.
#
# Asserts that the committed `stencils_x86_64.h` and `stencils_arm64.h`
# match what the pinned upstream clang produces from `stencils_src.c`.
# A diff means either:
#   (a) someone hand-edited a generated file, or
#   (b) the pinned clang was bumped without committing the regenerated
#       headers, or
#   (c) clang's codegen genuinely changed for the same version (rare,
#       worth investigating).
#
# Designed to drop into any CI runner — pure bash, no CI-vendor
# specifics. Typical invocation:
#
#   ./storage/ndb/src/kernel/blocks/dbtup/jit/extract_stencils/drift_check.sh \
#       prod_build /opt/homebrew/opt/llvm@20/bin/clang
#
# Exit codes:
#   0  — committed headers match what regen produces (PASS)
#   1  — drift detected; see git diff in stderr for details
#   2  — environment issue (missing build dir, clang not found, etc.)

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <build_dir> [clang_path]" >&2
  exit 2
fi

BUILD_DIR="$1"
CLANG_OVERRIDE="${2:-}"

if [[ ! -d "$BUILD_DIR" ]]; then
  echo "drift_check.sh: build dir '$BUILD_DIR' does not exist" >&2
  exit 2
fi

REPO_ROOT="$(cd "$BUILD_DIR" && git rev-parse --show-toplevel)"
JIT_REL="storage/ndb/src/kernel/blocks/dbtup/jit"
JIT_DIR="$REPO_ROOT/$JIT_REL"

if [[ ! -d "$JIT_DIR" ]]; then
  echo "drift_check.sh: jit dir not found at $JIT_DIR — is this a RonDB checkout?" >&2
  exit 2
fi

# If the working tree already has uncommitted changes to the generated
# headers, refuse to run — we'd corrupt them. Caller should commit or
# stash first.
if ! git -C "$REPO_ROOT" diff --quiet -- \
       "$JIT_REL/stencils_x86_64.h" "$JIT_REL/stencils_arm64.h"; then
  echo "drift_check.sh: refusing to run — generated headers already have uncommitted changes" >&2
  echo "  commit or stash $JIT_REL/stencils_*.h before running drift_check" >&2
  exit 2
fi

cd "$BUILD_DIR"

if [[ -n "$CLANG_OVERRIDE" ]]; then
  echo "drift_check.sh: reconfiguring with NDB_JIT_CLANG=$CLANG_OVERRIDE"
  cmake -DNDB_JIT_CLANG="$CLANG_OVERRIDE" . >/dev/null
fi

echo "drift_check.sh: running regen-stencils (clang preflight + extract + audit)"
if ! cmake --build . --target regen-stencils; then
  echo "drift_check.sh: regen-stencils failed (clang preflight or magic-byte audit?)" >&2
  exit 1
fi

# regen.cmake writes generated headers in-place into the source tree
# only if their bytes differ. So `git diff` against HEAD is the
# authoritative drift signal.
DIFF_OUTPUT="$(git -C "$REPO_ROOT" diff -- \
    "$JIT_REL/stencils_x86_64.h" "$JIT_REL/stencils_arm64.h" || true)"

if [[ -z "$DIFF_OUTPUT" ]]; then
  echo "drift_check.sh: PASS — committed headers match regenerated output."
  exit 0
fi

echo ""
echo "drift_check.sh: FAIL — committed headers drifted from regenerated output." >&2
echo "" >&2
echo "Diff:" >&2
echo "$DIFF_OUTPUT" >&2
echo "" >&2
echo "To resolve:" >&2
echo "  - If the change is intentional (stencils_src.c edit, clang pin bump):" >&2
echo "    commit the regenerated headers." >&2
echo "  - If unexpected: check NDB_JIT_CLANG --version, or look for hand-edits" >&2
echo "    to $JIT_REL/stencils_*.h." >&2

# Restore committed state so the runner's working tree is clean.
git -C "$REPO_ROOT" checkout -- \
    "$JIT_REL/stencils_x86_64.h" "$JIT_REL/stencils_arm64.h" || true

exit 1

#!/usr/bin/env bash
set -euo pipefail

# Purpose:
# - Run Prettier on files returned by `git diff --name-only`.
# - Keep CI focused on changed files while remaining flexible.
#
# Usage examples:
# - scripts/prettier-diff.sh
# - scripts/prettier-diff.sh --check
# - scripts/prettier-diff.sh --format
# - scripts/prettier-diff.sh --check HEAD~1..HEAD
# - scripts/prettier-diff.sh --write --cached

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PRETTIER_ARGS=()
HAS_MODE=false
DIFF_ARGS=()
for arg in "$@"; do
	case "$arg" in
		--format)
			PRETTIER_ARGS+=("--write")
			HAS_MODE=true
			;;
		--check|--write)
			PRETTIER_ARGS+=("$arg")
			HAS_MODE=true
			;;
		*)
			DIFF_ARGS+=("$arg")
			;;
	esac
done

if [[ "$HAS_MODE" = false ]]; then
	PRETTIER_ARGS=("--check" "${PRETTIER_ARGS[@]}")
fi

mapfile -t CHANGED < <(git diff --name-only --diff-filter=ACMRT "${DIFF_ARGS[@]}" \
	| grep -E '\.(js|cjs|mjs|ts|tsx|json|ya?ml|md)$' || true)

if [[ ${#CHANGED[@]} -eq 0 ]]; then
	echo "No supported files changed; skipping Prettier."
	exit 0
fi

echo "Running Prettier on ${#CHANGED[@]} file(s):"
printf ' - %s\n' "${CHANGED[@]}"

yarn run --silent prettier "${CHANGED[@]}" "${PRETTIER_ARGS[@]}"

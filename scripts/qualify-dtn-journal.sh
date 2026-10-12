#!/usr/bin/env bash
# Copyright (C) 2026 Luminous Dynamics
# SPDX-License-Identifier: Apache-2.0
# Focused exact-head qualification for the durable DTN receive/journal boundary.
# Re-triggered after the pinned Rust 1.96 formatting repair.

set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

actual_sha="$(git rev-parse HEAD)"
expected_sha="${QUALIFIED_SHA:-$actual_sha}"
head_tree="$(git rev-parse 'HEAD^{tree}')"
receipt="${DTN_JOURNAL_RECEIPT:-${TMPDIR:-/tmp}/dtn-journal-qualification-v1.tsv}"
status="FAIL"
stage="preflight"
source_state="unverified"

sha256_file() {
  local path="$1"
  if [[ -f "$path" ]]; then sha256sum "$path" | awk '{print $1}'; else printf 'unavailable'; fi
}

write_receipt() {
  local exit_code="$1"
  local final="$status"
  local terminal="$stage"
  [[ "$exit_code" -eq 0 ]] || final="FAIL"
  [[ "$final" == "PASS" ]] && terminal="none"
  mkdir -p "$(dirname "$receipt")"
  {
    printf 'schema\tdtn-journal-qualification-v1\n'
    printf 'status\t%s\n' "$final"
    printf 'exit_code\t%s\n' "$exit_code"
    printf 'terminal_stage\t%s\n' "$terminal"
    printf 'scope\tdtn-receive-journal-software-contract-only\n'
    printf 'full_ci\tindependent\n'
    printf 'real_daemon_smoke\tindependent\n'
    printf 'holochain_conductor_qualification\tnone\n'
    printf 'exactly_once_claim\tnone\n'
    printf 'authenticated_peer_identity_claim\tnone\n'
    printf 'qualified_sha\t%s\n' "$actual_sha"
    printf 'expected_sha\t%s\n' "$expected_sha"
    printf 'committed_tree\t%s\n' "$head_tree"
    printf 'source_state\t%s\n' "$source_state"
    printf 'runner_os\t%s\n' "${RUNNER_OS:-unknown}"
    printf 'runner_arch\t%s\n' "${RUNNER_ARCH:-unknown}"
    printf 'runner_image_os\t%s\n' "${ImageOS:-unknown}"
    printf 'runner_image_version\t%s\n' "${ImageVersion:-unknown}"
    printf 'rustc\t%s\n' "$(rustc -V 2>/dev/null || printf unavailable)"
    printf 'cargo\t%s\n' "$(cargo -V 2>/dev/null || printf unavailable)"
    printf 'cargo_lock_sha256\t%s\n' "$(sha256_file Cargo.lock)"
    printf 'cargo_manifest_sha256\t%s\n' "$(sha256_file Cargo.toml)"
    printf 'rust_toolchain_sha256\t%s\n' "$(sha256_file rust-toolchain.toml)"
    printf 'journal_source_sha256\t%s\n' "$(sha256_file src/journal.rs)"
    printf 'transport_source_sha256\t%s\n' "$(sha256_file src/lib.rs)"
    printf 'runtime_contract_sha256\t%s\n' "$(sha256_file tests/runtime_contract.rs)"
    printf 'qualifier_sha256\t%s\n' "$(sha256_file scripts/qualify-dtn-journal.sh)"
    printf 'workflow_sha256\t%s\n' "$(sha256_file .github/workflows/journal-contract.yml)"
    printf 'github_run_id\t%s\n' "${GITHUB_RUN_ID:-not-applicable}"
    printf 'github_run_attempt\t%s\n' "${GITHUB_RUN_ATTEMPT:-not-applicable}"
  } > "${receipt}.tmp.$$"
  mv "${receipt}.tmp.$$" "$receipt"
  echo "dtn-journal receipt=$receipt status=$final stage=$terminal"
}

finish() {
  local code=$?
  trap - EXIT
  if [[ "$code" -eq 0 && "$status" != "PASS" ]]; then code=1; fi
  write_receipt "$code" || true
  exit "$code"
}
trap finish EXIT

stage="preflight_exact_head"
[[ "$actual_sha" == "$expected_sha" ]] || { echo "exact-head mismatch" >&2; exit 1; }

stage="preflight_clean_tree"
git diff --quiet --ignore-submodules -- || { source_state="tracked-modifications-present"; exit 1; }
git diff --cached --quiet --ignore-submodules -- || { source_state="staged-modifications-present"; exit 1; }
[[ -z "$(git ls-files --others --exclude-standard)" ]] || { source_state="untracked-source-present"; exit 1; }
source_state="clean-exact-checkout"

stage="metadata"
cargo metadata --locked --no-deps --format-version 1 >/dev/null

stage="format"
cargo fmt --all -- --check

stage="check_default"
cargo check --all-targets --locked

stage="check_real_dtn_test_code"
cargo check --tests --features real-dtn-tests --locked

stage="clippy"
cargo clippy --all-targets --locked -- -D warnings

stage="unit_and_hermetic_contract_tests"
cargo test --locked

stage="journal_unit_tests"
cargo test --locked journal::tests:: -- --nocapture

stage="runtime_contract_tests"
cargo test --locked --test runtime_contract -- --nocapture

stage="postflight_source_immutability"
[[ "$(git rev-parse HEAD)" == "$actual_sha" ]] || { source_state="head-changed-during-tests"; exit 1; }
git diff --quiet --ignore-submodules -- || { source_state="tracked-source-mutated-during-tests"; exit 1; }
git diff --cached --quiet --ignore-submodules -- || { source_state="staged-source-mutated-during-tests"; exit 1; }
[[ -z "$(git ls-files --others --exclude-standard)" ]] || { source_state="untracked-source-created-during-tests"; exit 1; }
source_state="clean-exact-checkout-postflight"

status="PASS"
stage="complete"

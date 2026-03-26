# Contributing Governance

This repository uses an issue-driven, PR-first workflow to protect production ETL quality and architecture invariants.

## Core Policy

- `main` is protected and intended to receive changes through Pull Requests.
- Contributors must not push directly to `main`.
- Every Pull Request must be linked to an issue (for example: `Closes #123`).
- PRs should be focused to one concern (feature, fix, refactor) and include validation evidence.
- As a solo-maintainer setup, merge eligibility relies on required CI checks rather than mandatory human approvals.

## Required Contribution Flow

1. Create an issue describing problem/context and intended change.
2. Create a branch from `main`:
   - Recommended naming: `feat/<short-topic>`, `fix/<short-topic>`, `chore/<short-topic>`.
3. Implement the change and add/update tests.
4. Update `CHANGELOG.md` if behavior, contracts, schema, or data model are affected.
5. Open a Pull Request:
   - Use the PR template.
   - Reference the issue (`Closes #...`).
   - Include verification notes (tests/lint/manual checks).
6. Get review and merge after approval.

## ETL-Specific Expectations

- Preserve microservice invariants defined in `README.md`:
  - ETL remains self-contained.
  - PostgreSQL is the only runtime external dependency.
  - CLI/API capability parity is maintained when adding features.
- Keep ingestion behavior deterministic and idempotent where practical.
- For migrations, scheduler changes, or ingestion semantics changes, include rollback notes in the PR.

## Owner/Admin Bypass Policy

- Repository admins (owner) may bypass branch protections in exceptional cases.
- Bypass is for emergencies only (incident response, hotfix unblock, or repository recovery).
- After bypassing:
  - Document the reason in an issue or PR comment.
  - Open a follow-up PR/issue if additional cleanup or review is needed.

## Code of Review

- Prioritize correctness, data safety, and operational reliability.
- Request changes when architecture invariants, data contracts, or validation evidence are missing.

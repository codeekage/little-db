# Changelog

All notable changes to this project will be documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-28

Initial public release.

### Added
- Bitcask-style storage engine: append-only segments, in-memory keydir,
  manifest + hint files, single-writer with group commit, background
  compaction.
- Length-prefixed binary wire protocol (see `docs/SPEC.md`). Supported
  ops: `PING`, `PUT`, `GET`, `DELETE`, `BATCH`, `READKEYRANGE`, `STATS`.
- TCP server (`little-db serve`) with bounded range-scan concurrency
  and structured `slog` logging (`--log-level`, `--log-format`).
- Pure-stdlib Go client library (`internal/client`).
- `little-db` CLI with subcommands: `serve`, `ping`, `put`, `get`,
  `delete`, `batch`, `range`, `stats`. See `docs/cli.md`.
- Test fixtures under `testdata/` and a generator script for bulk
  NDJSON inputs.

[Unreleased]: https://github.com/codeekage/little-db/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/codeekage/little-db/releases/tag/v0.1.0

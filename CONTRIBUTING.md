# Contributing

Thanks for considering a contribution. A few ground rules keep the
project boring (in a good way) to maintain.

## Development setup

```bash
git clone https://github.com/codeekage/little-db.git
cd little-db
make verify   # build + vet + test
```

Requires Go 1.22+. No external dependencies — everything is stdlib.

## Before opening a PR

- `make verify` must pass locally.
- Add or update tests for any behavior change. The wire protocol is
  considered frozen; protocol-affecting changes need a separate
  discussion before code.
- Keep commits focused. Conventional Commits style preferred
  (`feat:`, `fix:`, `docs:`, `chore:`, etc.).
- Update `docs/cli.md` if you add or rename a flag, and `docs/SPEC.md`
  if you touch on-disk or wire formats.

## Scope

This is a teaching-grade Bitcask-style KV store. Contributions that
keep the codebase small, dependency-free, and reviewable are welcome.
Contributions that add framework dependencies, ORM-style sugar, or
optional features behind build tags are unlikely to land — please
open an issue to discuss before writing code.

## Reporting bugs

Use GitHub Issues. Include the commit SHA, the exact command, and the
full stderr output. For security issues, see [SECURITY.md](SECURITY.md).

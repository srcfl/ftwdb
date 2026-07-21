# Release policy and process

FTWDB uses Semantic Versioning for the crate, tags, and GitHub releases. A tag
has the form `vMAJOR.MINOR.PATCH` or
`vMAJOR.MINOR.PATCH-(alpha|beta|rc).NUMBER`. The version in `Cargo.toml`, the
changelog heading, and the tag without its leading `v` must match exactly.

## Release channels

- **Alpha** is for evaluation. Core safety tests and packaging must pass. Known
  hardware, scale, and support gaps must appear in the changelog.
- **Beta** requires target-board and physical SD-card tests, power cuts during
  commits, a long soak run, and a tested operator backup policy.
- **Release candidate** freezes the public API and on-disk format for the
  planned stable release. Only release-blocking fixes may follow.
- **Stable** requires the relevant roadmap exit, a stated compatibility and
  support window, and no open release-blocking defect.

Pre-release versions may break APIs and formats between releases. Stable
versions follow the compatibility promise published with that release.

## Tag rules

Release tags point to a commit on `main` after a release-prep pull request has
passed CI. Create annotated tags so the tag records its release notes. Sign a
tag only when the maintainer has a configured key whose public identity users
can check; never claim an unsigned tag is signed.

Never move, reuse, or replace a published tag. If a release has a defect, mark
it as withdrawn in its notes and publish the next version. Delete a tag only to
remove exposed secrets or meet a legal requirement, and record that event.

## Required alpha checks

1. The working tree starts from current `origin/main`.
2. GitHub has no open release-blocking issue or pull request.
3. `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` name the same version.
4. Format, Clippy, all targets and features, documentation tests, Python
   adapter tests, and SD-emulator tests pass with locked dependencies.
5. `cargo package --locked` builds and verifies the crate.
6. Release notes list platform support and all known safety gaps.
7. The release-prep pull request passes Linux and macOS CI and merges to
   `main` before the tag is created.

## Automated publication

Pushing a matching tag starts `.github/workflows/release.yml`. The workflow:

1. checks that the tag is annotated and matches the exact Cargo/changelog
   version;
2. repeats the release test and package checks;
3. builds native `ftw` archives on GitHub's Linux and macOS runners;
4. creates `SHA256SUMS` for the archives; and
5. creates a GitHub prerelease when the tag contains a pre-release suffix.

The archive name includes the tag and Rust host target. Each archive contains
the `ftw` binary, README, changelog, and license. GitHub Actions records the
source commit and workflow run. Alpha releases stay on GitHub; publishing the
crate to crates.io needs a separate decision and an explicit publish step.

## Maintainer steps

1. Create a release-prep branch from `origin/main`.
2. Update the Cargo version, changelog, docs, and any compatibility statement.
3. Run all required checks, open a pull request, and wait for CI.
4. Merge the pull request and update local remote references.
5. Create an annotated tag on the exact merge commit with concise release
   notes and known limits.
6. Push only that tag and watch the Release workflow to completion.
7. Check the GitHub release, both archives, `SHA256SUMS`, and the source commit.

Do not create a release from a feature branch, a dirty tree, or a commit whose
CI result is unknown.

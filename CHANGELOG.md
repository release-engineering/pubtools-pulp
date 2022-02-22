# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- n/a

## [1.7.1] - 2022-02-23

- Improved performance of `pubtools-pulp-push`.

## [1.7.0] - 2022-02-09

- `pubtools-pulp-push` now records push items (via `pushcollector` library).

## [1.6.0] - 2022-01-31

### Added

- Introduced `pubtools-pulp-fix-cves` command

## [1.5.0] - 2022-01-21

- Introduced `task_pulp_flush` hook.
- `pubtools-pulp-push` now accepts a `--pre-push` argument.

## [1.4.0] - 2021-12-17

- Introduced `pubtools-pulp-push` command.

## [1.3.0] - 2021-11-02

- The `--pulp-fake` option was added for development and testing.
- Pulp throttling may now be controlled via the `PULP_THROTTLE` environment variable.

## [1.2.0] - 2021-08-18

### Changed

- clear-repo now accepts the `--content-type` argument multiple times.
- Internally created executors are now named for improved metrics and debuggability.

## [1.1.0] - 2021-03-22

### Added
- Publish command accepts multiple repo-ids arg
- Add pulp-throttle arg for all commands that limit number of pulp tasks running simultaneously

## [1.0.3] - 2021-02-05

- Updated clear-repo to send pushitem objects to the collector

## [1.0.2] - 2020-04-02

- Fixed a crash when clear-repo attempts to flush engproduct in UD

## [1.0.1] - 2020-03-26

- Fixed compatibility with previous versions of concurrent.futures.ThreadPoolExecutor

## [1.0.0] - 2019-10-17

- Publish and maintenance-on/off commands accept comma seperated repo-ids

## 0.1.0 - 2019-10-09

- Initial release to PyPI

[Unreleased]: https://github.com/release-engineering/pubtools-pulp/compare/v1.7.1...HEAD
[1.7.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.7.0...v1.7.1
[1.7.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.0.3...v1.1.0
[1.0.3]: https://github.com/release-engineering/pubtools-pulp/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/release-engineering/pubtools-pulp/compare/v0.1.0...v1.0.0

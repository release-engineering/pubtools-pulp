# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- n/a

## [1.12.0] - 2022-04-24

- `pubtools-pulp-delete`: packages in advisory are deleted from all repos
  when no repos provided

## [1.11.0] - 2022-04-08

- Introduced `pubtools-pulp-delete` command

## [1.10.1] - 2022-04-07

- `pubtools-pulp-push`: push items with no destination are now skipped
  rather than triggering a crash

## [1.10.0] - 2022-03-30

- `pubtools-pulp-push`: fixed uploads of productid and similar content not
  being awaited or error-checked
- `pubtools-pulp-push`: fixed handling of non-Pulp destinations of push
  items
- `pubtools-pulp-push`: added support for RPM signature filtering
  and `--allow-unsigned` option
- `pubtools-pulp-push`: upload of productid now adjusts product_versions
  note on Pulp repos

## [1.9.3] - 2022-03-17

- `pubtools-pulp-push`: when the same file exists multiple times in a push, it will no
  longer be uploaded to Pulp more than once.

## [1.9.2] - 2022-03-15

- `pubtools-pulp-push`: improved pipelining behavior for larger pushes
- `pubtools-pulp-push`: fixed duplicate push item metadata

## [1.9.1] - 2022-03-09

- Added some caching of Pulp repo lookups to improve performance
- `pubtools-pulp-push`: fixed attempts to flush UD cache for all-rpm-content
- `pubtools-pulp-push`: fixed a scaling issue which could lead to stack overflow on
  pushes with large numbers of items

## [1.9.0] - 2022-03-08

- Reduced memory usage by using slotted classes
- `pubtools-pulp-push`: minor performance improvements
- `pubtools-pulp-push`: minor logging improvements in some error handling scenarios

## [1.8.2] - 2022-03-03

- `pubtools-pulp-push`: improved the pipelining of each push phase.
- `pubtools-pulp-push`: improved the accuracy of progress logging.
- `pubtools-pulp-push`: fix: progress logs did not respect `COLUMNS` environment
                        variable when running on python2

## [1.8.1] - 2022-03-02

- `pubtools-pulp-push`: improved the performance of the checksum calculation step.

## [1.8.0] - 2022-02-25

- `pubtools-pulp-push` now logs detailed progress information periodically.

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

[Unreleased]: https://github.com/release-engineering/pubtools-pulp/compare/v1.12.0...HEAD
[1.12.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.11.0...v1.12.0
[1.11.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.10.1...v1.11.0
[1.10.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.10.0...v1.10.1
[1.10.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.9.3...v1.10.0
[1.9.3]: https://github.com/release-engineering/pubtools-pulp/compare/v1.9.2...v1.9.3
[1.9.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.9.1...v1.9.2
[1.9.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.9.0...v1.9.1
[1.9.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.8.2...v1.9.0
[1.8.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.8.1...v1.8.2
[1.8.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.7.1...v1.8.0
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

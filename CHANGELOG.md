# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- n/a

## [1.33.2] - 2025-04-10

- Re-enabled flushing UD cache by Engineering ID number

## [1.33.1] - 2025-03-24

- Stopped flushing UD cache by Engineering ID number
- Push items are not recorded for `pubtools-pulp-copy-repo` task

## [1.33.0] - 2025-03-03

- Updated content upload to push bits provided by pushitem's `content()`

## [1.32.2] - 2025-01-27

- Fixed `pubtools-pulp-copy-repo` failure for different repo type pairs in same request
- Added docs for `pubtools-pulp-copy-repo`

## [1.32.1] - 2025-01-14

- Improved `pubtools-pulp-copy-repo` performance reducing resource consumption

## [1.32.0] - 2024-11-22

- Improved `pubtools-pulp-delete` to use `--repo *` for selecting all repos
- Modular rpms are deleted from all provided repos in `pubtools-pulp-delete`
- Fixed crash when trying to skip publish of repos

## [1.31.0] - 2024-07-01

- Fixed erratum unit upload to `all-erratum-content-YYYY` that affected the pkglist generation
- Fixed the init namespace conflict with other pubtools* libraries
- Added UD cache flush skip for repositories without engIDs
- Retired CDN Cache flush support via fastpurge
- Introduced cert/key authentication support for UD Cache client

## [1.30.1] - 2024-05-16

- Excluded `all-erratum-content-YYYY` from publishing
- Fixed `pubtools-pulp-copy-repo` task - parameters handling and copies submission
- `ProductID` certificate is now loaded via `pushsource` lib

## [1.29.1] - 2024-04-15

- Added support for `all-rpm-content-XX` and `all-erratum-content-YYYY` repositories in `upload_repo()`

## [1.28.1] - 2024-04-05

- Added retries for copy operation in `pubtools-pulp-push`
- Introduced mechanism for implicit skip of steps in a task

## [1.27.1] - 2024-02-22

- Introduced `pubtools-pulp-copy-repo` command

## [1.26.5] - 2023-11-29

- Improved `pubtools-pulp-garbage-collect` effectiveness

## [1.26.4] - 2023-11-10

- Fixed TTL extraction from cache key header

## [1.26.3] - 2023-09-29

- Fixed handling of `--cdn-arl-template` option with multiple values.

## [1.26.2] - 2023-09-27

- Improved logging during CDN cache flushing.

## [1.26.1] - 2023-06-08

- Fixed a minor logging error during Pulp authentication.

## [1.26.0] - 2023-04-26

- `pubtools-pulp-publish`: the `--published-before` option now supports timestamps.

## [1.25.0] - 2023-04-20

- Added purging CDN cache by ARL

## [1.24.0] - 2023-03-28

- `pubtools-pulp-maintenance-on/off` now use advisory locks to avoid conflicting updates
  when several tasks are run concurrently.

## [1.23.0] - 2023-03-23

- Added UD cache flush of Erratum object in CVE fix task

## [1.22.0] - 2023-03-01

- Added container_list converter for ErratumUnit

## [1.21.0] - 2023-02-16

- Added PKI support for Pulp client

## [1.20.3] - 2022-12-20

- Fixed issues with usage of `monotonic` dependency

## [1.20.2] - 2022-11-21

- Fixed pulp_item_push_finished hook arguments

## [1.20.1] - 2022-09-27

- `pubtools-pulp-push`: fix spurious push timeout after ~55 hours

## [1.20.0] - 2022-08-02

- `pubtools-pulp-push`: errata's always uploaded to `all-rpm-content` repository

## [1.19.0] - 2022-07-27

- Added retry args to increase retry backoff in UD Cache client.

## [1.18.0] - 2022-07-19

- Added errata flush to UD cache flush step post publish.

## [1.17.0] - 2022-06-16

- Introduced `pulp_item_push_finished`
  [pubtools hook](https://release-engineering.github.io/pubtools/hooks.html#hook-reference).

## [1.16.0] - 2022-06-07

- Internal refactoring of `pubtools-pulp-push` to improve memory usage, error handling and
  miscellaneous issues.

## [1.15.0] - 2022-05-13

- `pubtools-pulp-push` now supports a `--skip` argument.

## [1.14.0] - 2022-05-10

- Internal refactoring to reduce memory usage for large pushes.

## [1.13.0] - 2022-04-28

- `pubtools-pulp-garbage-collect` will now remove stale content from the
  `all-rpm-content` repository

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

[Unreleased]: https://github.com/release-engineering/pubtools-pulp/compare/v1.33.2...HEAD
[1.33.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.33.1...v1.33.2
[1.33.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.33.0...v1.33.1
[1.33.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.32.2...v1.33.0
[1.32.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.32.1...v1.32.2
[1.32.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.32.0...v1.32.1
[1.32.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.31.0...v1.32.0
[1.31.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.30.1...v1.31.0
[1.30.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.29.1...v1.30.1
[1.29.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.28.1...v1.29.1
[1.28.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.27.1...v1.28.1
[1.27.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.5...v1.27.1
[1.26.5]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.4...v1.26.5
[1.26.4]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.3...v1.26.4
[1.26.3]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.2...v1.26.3
[1.26.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.1...v1.26.2
[1.26.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.26.0...v1.26.1
[1.26.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.25.0...v1.26.0
[1.25.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.24.0...v1.25.0
[1.24.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.23.0...v1.24.0
[1.23.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.22.0...v1.23.0
[1.22.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.21.0...v1.22.0
[1.21.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.20.3...v1.21.0
[1.20.3]: https://github.com/release-engineering/pubtools-pulp/compare/v1.20.2...v1.20.3
[1.20.2]: https://github.com/release-engineering/pubtools-pulp/compare/v1.20.1...v1.20.2
[1.20.1]: https://github.com/release-engineering/pubtools-pulp/compare/v1.20.0...v1.20.1
[1.20.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.19.0...v1.20.0
[1.19.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.18.0...v1.19.0
[1.18.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.17.0...v1.18.0
[1.17.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.16.0...v1.17.0
[1.16.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.15.0...v1.16.0
[1.15.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.14.0...v1.15.0
[1.14.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.13.0...v1.14.0
[1.13.0]: https://github.com/release-engineering/pubtools-pulp/compare/v1.12.0...v1.13.0
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

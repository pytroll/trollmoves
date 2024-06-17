## Version 0.15.1 (2024/06/17)


### Pull Requests Merged

#### Bugs fixed

* [PR 204](https://github.com/pytroll/trollmoves/pull/204) - Fix tests for newer fsspec
* [PR 202](https://github.com/pytroll/trollmoves/pull/202) - Fix removal topic
* [PR 200](https://github.com/pytroll/trollmoves/pull/200) - Fix naive timestamp

In this release 3 pull requests were closed.


## Version 0.15.0 (2024/05/27)

### Issues Closed

* [Issue 194](https://github.com/pytroll/trollmoves/issues/194) - move it server fails to start when config uses listen instead of origin ([PR 195](https://github.com/pytroll/trollmoves/pull/195) by [@mraspaud](https://github.com/mraspaud))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 195](https://github.com/pytroll/trollmoves/pull/195) - Fix posttroll-based notifier for move it server ([194](https://github.com/pytroll/trollmoves/issues/194))

#### Features added

* [PR 199](https://github.com/pytroll/trollmoves/pull/199) - Replace utcnow (and fix style along the way)
* [PR 198](https://github.com/pytroll/trollmoves/pull/198) - Add the fetcher functionality and documentation on rtd

In this release 3 pull requests were closed.


## Version 0.14.0 (2024/02/28)

### Issues Closed

* [Issue 192](https://github.com/pytroll/trollmoves/issues/192) - Migrate away from `pyinotify` ([PR 185](https://github.com/pytroll/trollmoves/pull/185) by [@mraspaud](https://github.com/mraspaud))
* [Issue 189](https://github.com/pytroll/trollmoves/issues/189) - S3 mover passes all config items to `S3FileSystem()` ([PR 190](https://github.com/pytroll/trollmoves/pull/190) by [@pnuu](https://github.com/pnuu))

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 190](https://github.com/pytroll/trollmoves/pull/190) - Fix connection parameter handling ([189](https://github.com/pytroll/trollmoves/issues/189))
* [PR 184](https://github.com/pytroll/trollmoves/pull/184) - Make sure unpack takes `delete` as a boolean
* [PR 183](https://github.com/pytroll/trollmoves/pull/183) - Update versioneer

#### Features added

* [PR 193](https://github.com/pytroll/trollmoves/pull/193) - Use log file in tmp_path instead of 'somefile'
* [PR 191](https://github.com/pytroll/trollmoves/pull/191) - Update python CI versions to cover 3.10 - 3.12
* [PR 190](https://github.com/pytroll/trollmoves/pull/190) - Fix connection parameter handling ([189](https://github.com/pytroll/trollmoves/issues/189))
* [PR 185](https://github.com/pytroll/trollmoves/pull/185) - Replace pyinotify with watchdog ([192](https://github.com/pytroll/trollmoves/issues/192))

In this release 7 pull requests were closed.


## Version 0.13.1 (2023/09/04)

### Issues Closed

* [Issue 175](https://github.com/pytroll/trollmoves/issues/175) - urlparse in move_it function fails to parse destination parameter when destination parameter already is a urllib.parse.ParseResult ([PR 176](https://github.com/pytroll/trollmoves/pull/176) by [@TAlonglong](https://github.com/TAlonglong))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 176](https://github.com/pytroll/trollmoves/pull/176) - Handle destination as urlparse type. Add test. ([175](https://github.com/pytroll/trollmoves/issues/175))
* [PR 174](https://github.com/pytroll/trollmoves/pull/174) - Fix ftp mover using destination filename when provided

In this release 2 pull requests were closed.


###############################################################################
## Version 0.13.0 (2023/06/05)

### Issues Closed

* [Issue 171](https://github.com/pytroll/trollmoves/issues/171) - Publisher need to call start method as of version 1.10.0 of posttroll
* [Issue 169](https://github.com/pytroll/trollmoves/issues/169) - In the server add backup target host for scp ([PR 170](https://github.com/pytroll/trollmoves/pull/170) by [@TAlonglong](https://github.com/TAlonglong))

In this release 2 issues were closed.

### Pull Requests Merged

#### Features added

* [PR 170](https://github.com/pytroll/trollmoves/pull/170) - adding backup targets ([169](https://github.com/pytroll/trollmoves/issues/169))

In this release 1 pull request was closed.

###############################################################################
## Version 0.12.0 (2023/04/27)

### Issues Closed

* [Issue 123](https://github.com/pytroll/trollmoves/issues/123) - s3downloader: download data from an s3 (object store) to localhost ([PR 124](https://github.com/pytroll/trollmoves/pull/124) by [@TAlonglong](https://github.com/TAlonglong))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 167](https://github.com/pytroll/trollmoves/pull/167) - Fix installation of the S3 downloader script
* [PR 166](https://github.com/pytroll/trollmoves/pull/166) - Error in destination path when dispatching to S3

#### Features added

* [PR 166](https://github.com/pytroll/trollmoves/pull/166) - Error in destination path when dispatching to S3
* [PR 165](https://github.com/pytroll/trollmoves/pull/165) - Make it possible to pass connection-parameters to the S3 mover
* [PR 164](https://github.com/pytroll/trollmoves/pull/164) - Remove unnecessary codecov python package install in CI
* [PR 163](https://github.com/pytroll/trollmoves/pull/163) - Handle the distapcthing of a dataset (keeping the same filenames)
* [PR 124](https://github.com/pytroll/trollmoves/pull/124) - Add S3 downloader ([123](https://github.com/pytroll/trollmoves/issues/123))

In this release 7 pull requests were closed.

## Version 0.11.0 (2023/03/27)

### Issues Closed

* [Issue 146](https://github.com/pytroll/trollmoves/issues/146) - Add timeout to ssh connect ([PR 147](https://github.com/pytroll/trollmoves/pull/147) by [@TAlonglong](https://github.com/TAlonglong))
* [Issue 141](https://github.com/pytroll/trollmoves/issues/141) - delete default set to bool False, but is handled as a string ([PR 142](https://github.com/pytroll/trollmoves/pull/142) by [@TAlonglong](https://github.com/TAlonglong))
* [Issue 111](https://github.com/pytroll/trollmoves/issues/111) - Decompression by Server fails the transfer

In this release 3 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 152](https://github.com/pytroll/trollmoves/pull/152) - Do not transform local paths to ssh-uris
* [PR 142](https://github.com/pytroll/trollmoves/pull/142) - Fix bug handling boolean as string ([141](https://github.com/pytroll/trollmoves/issues/141))
* [PR 140](https://github.com/pytroll/trollmoves/pull/140) - Add try/except around Message creation
* [PR 128](https://github.com/pytroll/trollmoves/pull/128) - Add pyinotify a hard requirement

#### Features added

* [PR 157](https://github.com/pytroll/trollmoves/pull/157) - Add file creation time as metadata
* [PR 147](https://github.com/pytroll/trollmoves/pull/147) - Add timeout to ssh client connect ([146](https://github.com/pytroll/trollmoves/issues/146))
* [PR 134](https://github.com/pytroll/trollmoves/pull/134) - Import `netifaces` only if used
* [PR 133](https://github.com/pytroll/trollmoves/pull/133) - Refactor the logging
* [PR 132](https://github.com/pytroll/trollmoves/pull/132) - Modernize sftp mover
* [PR 130](https://github.com/pytroll/trollmoves/pull/130) - Add a skeleton documentation for Trollmoves
* [PR 127](https://github.com/pytroll/trollmoves/pull/127) - Use dictionary config interfaces for publisher and subscriber creation

In this release 11 pull requests were closed.


###############################################################################
## Version 0.10.0 (2022/08/11)

### Issues Closed

* [Issue 116](https://github.com/pytroll/trollmoves/issues/116) - Adjust move_it.py to use movers from trollmoves.movers ([PR 117](https://github.com/pytroll/trollmoves/pull/117) by [@pnuu](https://github.com/pnuu))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 113](https://github.com/pytroll/trollmoves/pull/113) - Fix dispatcher destination url creation and revert test config to intended value

#### Features added

* [PR 119](https://github.com/pytroll/trollmoves/pull/119) - Add an option to disable target directory creation
* [PR 117](https://github.com/pytroll/trollmoves/pull/117) - Use movers from the trollmoves package ([116](https://github.com/pytroll/trollmoves/issues/116))
* [PR 115](https://github.com/pytroll/trollmoves/pull/115) - Add a mover for s3:// protocol
* [PR 112](https://github.com/pytroll/trollmoves/pull/112) - Remove Python 3.7 and add Python 3.10 for running unittests

In this release 5 pull requests were closed.


###############################################################################
## Version v0.9.0 (2021/12/03)

### Issues Closed

* [Issue 107](https://github.com/pytroll/trollmoves/issues/107) - Possible to handle messages with dataset
* [Issue 100](https://github.com/pytroll/trollmoves/issues/100) - Refactor unit tests ([PR 101](https://github.com/pytroll/trollmoves/pull/101) by [@pnuu](https://github.com/pnuu))
* [Issue 97](https://github.com/pytroll/trollmoves/issues/97) - Adjusting the config restarts Client listeners unnecessarily ([PR 99](https://github.com/pytroll/trollmoves/pull/99) by [@pnuu](https://github.com/pnuu))
* [Issue 80](https://github.com/pytroll/trollmoves/issues/80) - Move child classes of MoveItBase to respective modules within trollmoves library

In this release 4 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 108](https://github.com/pytroll/trollmoves/pull/108) - Refactor dispatcher
* [PR 99](https://github.com/pytroll/trollmoves/pull/99) - Prevent unnecessary listener reloads in Trollmoves Client ([97](https://github.com/pytroll/trollmoves/issues/97))

#### Features added

* [PR 109](https://github.com/pytroll/trollmoves/pull/109) - Example end-to-end test
* [PR 108](https://github.com/pytroll/trollmoves/pull/108) - Refactor dispatcher
* [PR 106](https://github.com/pytroll/trollmoves/pull/106) - Refactor mirror
* [PR 105](https://github.com/pytroll/trollmoves/pull/105) - Refactor server
* [PR 104](https://github.com/pytroll/trollmoves/pull/104) - Refactor client
* [PR 103](https://github.com/pytroll/trollmoves/pull/103) - Remove client listener callback
* [PR 102](https://github.com/pytroll/trollmoves/pull/102) - Remove six usage, reorder imports and fix flake8 warnings
* [PR 101](https://github.com/pytroll/trollmoves/pull/101) - Refactor unit tests ([100](https://github.com/pytroll/trollmoves/issues/100))
* [PR 99](https://github.com/pytroll/trollmoves/pull/99) - Prevent unnecessary listener reloads in Trollmoves Client ([97](https://github.com/pytroll/trollmoves/issues/97))

In this release 11 pull requests were closed.

## Version 0.8.1 (2021/11/04)

### Issues Closed

* [Issue 85](https://github.com/pytroll/trollmoves/issues/85) - Trollmoves Client shutsdown when config is modified ([PR 96](https://github.com/pytroll/trollmoves/pull/96) by [@pnuu](https://github.com/pnuu))

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 96](https://github.com/pytroll/trollmoves/pull/96) - Stop Publisher restarting when publisher parameters are not changed ([85](https://github.com/pytroll/trollmoves/issues/85))

#### Features added

* [PR 96](https://github.com/pytroll/trollmoves/pull/96) - Stop Publisher restarting when publisher parameters are not changed ([85](https://github.com/pytroll/trollmoves/issues/85))

In this release 2 pull requests were closed.


## Version 0.8.0 (2021/11/01)


### Pull Requests Merged

#### Bugs fixed

* [PR 95](https://github.com/pytroll/trollmoves/pull/95) - Bugfix spare client requests

#### Features added

* [PR 95](https://github.com/pytroll/trollmoves/pull/95) - Bugfix spare client requests

In this release 2 pull requests were closed.


## Version v0.7.0 (2021/08/25)

### Issues Closed

* [Issue 88](https://github.com/pytroll/trollmoves/issues/88) - Trollmoves Client leaks memory ([PR 89](https://github.com/pytroll/trollmoves/pull/89) by [@pnuu](https://github.com/pnuu))
* [Issue 86](https://github.com/pytroll/trollmoves/issues/86) - Re-establishing connection for Trollmoves Client after network glitches
* [Issue 81](https://github.com/pytroll/trollmoves/issues/81) - Not answering an invalid request could lock the client ([PR 82](https://github.com/pytroll/trollmoves/pull/82) by [@mraspaud](https://github.com/mraspaud))

In this release 3 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 94](https://github.com/pytroll/trollmoves/pull/94) - Fix attribute passing to the mirror deleter
* [PR 89](https://github.com/pytroll/trollmoves/pull/89) - Ensure transfers are cleared from ongoing transfers after duplicate messages ([88](https://github.com/pytroll/trollmoves/issues/88))
* [PR 84](https://github.com/pytroll/trollmoves/pull/84) - Fix publisher handling

#### Features added

* [PR 83](https://github.com/pytroll/trollmoves/pull/83) - Add configurable poll interval (timeout) to watchdog poller
* [PR 82](https://github.com/pytroll/trollmoves/pull/82) - Try to reply to invalid messages ([81](https://github.com/pytroll/trollmoves/issues/81))

In this release 5 pull requests were closed.


## Version 0.6.2 (2020/12/02)

### Pull Requests Merged

#### Bugs fixed
* [PR 79](https://github.com/pytroll/trollmoves/pull/79) - Fix publisher name for MoveItServer

In this release 1 pull request was closed.


## Version 0.6.1 (2020/06/08)

### Issues Closed

* [Issue 77](https://github.com/pytroll/trollmoves/issues/77) - Use correct publishers ([PR 76](https://github.com/pytroll/trollmoves/pull/76))
* [Issue 74](https://github.com/pytroll/trollmoves/issues/74) - KeyError 'directory' ([PR 75](https://github.com/pytroll/trollmoves/pull/75))

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 76](https://github.com/pytroll/trollmoves/pull/76) - Fix keyword argument naming for publisher ([77](https://github.com/pytroll/trollmoves/issues/77))
* [PR 75](https://github.com/pytroll/trollmoves/pull/75) - Fix missing default option making the dispatcher crash ([74](https://github.com/pytroll/trollmoves/issues/74))

#### Features added

* [PR 78](https://github.com/pytroll/trollmoves/pull/78) - Get username and password from .netrc file if available

In this release 3 pull requests were closed.

## Version 0.6.0 (2020/06/03)

### Issues Closed

* [Issue 72](https://github.com/pytroll/trollmoves/issues/72) - filepattern is a required parameter - should be optional ([PR 73](https://github.com/pytroll/trollmoves/pull/73))
* [Issue 54](https://github.com/pytroll/trollmoves/issues/54) -  connection lifetime should be configurable for all transfer methods

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 73](https://github.com/pytroll/trollmoves/pull/73) - Make filepattern optional ([72](https://github.com/pytroll/trollmoves/issues/72))

#### Features added

* [PR 62](https://github.com/pytroll/trollmoves/pull/62) - Add Watchdog based event handler

In this release 2 pull requests were closed.

## Version <v0.5.0> (2020/04/03)

### Issues Closed

* [Issue 40](https://github.com/pytroll/trollmoves/issues/40) - Make server and client aware of hot spare processes
* [Issue 39](https://github.com/pytroll/trollmoves/issues/39) - Make unpacking possible on both client and server

In this release 2 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 60](https://github.com/pytroll/trollmoves/pull/60) - Fix iterate_messages() generator raising StopIteration
* [PR 57](https://github.com/pytroll/trollmoves/pull/57) - Reload only the actual config file when it is modified

#### Features added

* [PR 61](https://github.com/pytroll/trollmoves/pull/61) - Create new compose-tags with aliases
* [PR 59](https://github.com/pytroll/trollmoves/pull/59) - Fix rpm names for newer distros
* [PR 58](https://github.com/pytroll/trollmoves/pull/58) - Publish messages after dispatch
* [PR 57](https://github.com/pytroll/trollmoves/pull/57) - Reload only the actual config file when it is modified

In this release 6 pull requests were closed.

## Version v0.4.0 (2020/02/10)

### Issues Closed

* [Issue 56](https://github.com/pytroll/trollmoves/issues/56) - All `push`, `ack` and `file` messages are published

In this release 1 issue was closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 55](https://github.com/pytroll/trollmoves/pull/55) - Fix dispatcher transfers for scp protocol
* [PR 53](https://github.com/pytroll/trollmoves/pull/53) - Fix unpack_tar() to return a tuple or string
* [PR 51](https://github.com/pytroll/trollmoves/pull/51) - Fix deletion of compressed files on the client
* [PR 50](https://github.com/pytroll/trollmoves/pull/50) - Hotfix client decompression when decompression not defined

#### Features added

* [PR 52](https://github.com/pytroll/trollmoves/pull/52) - Add bzip decompression to client
* [PR 49](https://github.com/pytroll/trollmoves/pull/49) - Add more decompression methods to client
* [PR 46](https://github.com/pytroll/trollmoves/pull/46) - Add config option for ssh port. Defaults to 22
* [PR 44](https://github.com/pytroll/trollmoves/pull/44) - Expose ListenerContainer config items
* [PR 43](https://github.com/pytroll/trollmoves/pull/43) - Fix yaml loading and hooks usage for dispatcher
* [PR 42](https://github.com/pytroll/trollmoves/pull/42) - Hot-spare client(s)
* [PR 41](https://github.com/pytroll/trollmoves/pull/41) - Accept log configs in dispatcher

In this release 11 pull requests were closed.

## Version 0.3.0 (2019/09/25)

### Issues Closed

* [Issue 38](https://github.com/pytroll/trollmoves/issues/38) - Add a decompressor for tar files

In this release 1 issue was closed.

### Pull Requests Merged

#### Features added

* [PR 37](https://github.com/pytroll/trollmoves/pull/37) - Add dispatcher utility

In this release 1 pull request was closed.

## Version 0.2.0 (2019/08/12)


### Pull Requests Merged

#### Bugs fixed

* [PR 36](https://github.com/pytroll/trollmoves/pull/36) - Prevent concurrent transfers when multiples sources are available
* [PR 35](https://github.com/pytroll/trollmoves/pull/35) - Fix move_it function to run with empty relative path
* [PR 31](https://github.com/pytroll/trollmoves/pull/31) - Bugfix: imports RawConfigParser rather than ConfigParser
* [PR 28](https://github.com/pytroll/trollmoves/pull/28) - Fix archive URI not being removed when unpacking
* [PR 27](https://github.com/pytroll/trollmoves/pull/27) - Fix error handling for client unpacking

#### Features added

* [PR 33](https://github.com/pytroll/trollmoves/pull/33) - Adding .stickler.yml configuration file
* [PR 32](https://github.com/pytroll/trollmoves/pull/32) - Add "nameservers" option to trollmoves client
* [PR 30](https://github.com/pytroll/trollmoves/pull/30) - Add config option for SSH key file
* [PR 20](https://github.com/pytroll/trollmoves/pull/20) - Restructure TrollMoves scripts

In this release 9 pull requests were closed.

## Version 0.1.3 (2019/04/10)

### Pull Requests Merged

#### Bugs fixed

* [PR 26](https://github.com/pytroll/trollmoves/pull/26) - Fix client untaring
* [PR 25](https://github.com/pytroll/trollmoves/pull/25) - Fix for new posttroll version, replacing context with get_context

In this release 2 pull requests were closed.

## Version 0.1.2 (2019/01/21)


### Pull Requests Merged

#### Bugs fixed

* [PR 23](https://github.com/pytroll/trollmoves/pull/23) - Fix the persistant ftp connection

#### Features added

* [PR 24](https://github.com/pytroll/trollmoves/pull/24) - Switch to versioneer
* [PR 22](https://github.com/pytroll/trollmoves/pull/22) - Remove Develop branch
* [PR 21](https://github.com/pytroll/trollmoves/pull/21) - Feature persistent connection
* [PR 17](https://github.com/pytroll/trollmoves/pull/17) - Feature posttroll serve
* [PR 16](https://github.com/pytroll/trollmoves/pull/16) - persistent ssh/scp connection
* [PR 15](https://github.com/pytroll/trollmoves/pull/15) - Make ftp connections persistent for 30 seconds
* [PR 14](https://github.com/pytroll/trollmoves/pull/14) - Feature client unpack
* [PR 13](https://github.com/pytroll/trollmoves/pull/13) - Improve error handling in SCPMover
* [PR 12](https://github.com/pytroll/trollmoves/pull/12) - Add support for Python 3
* [PR 11](https://github.com/pytroll/trollmoves/pull/11) - Added argument option disable-backlog
* [PR 8](https://github.com/pytroll/trollmoves/pull/8) - Introducing 'info' request

In this release 12 pull requests were closed.

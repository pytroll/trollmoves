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

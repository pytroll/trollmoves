Changelog
=========


v0.1.1 (2017-11-13)
-------------------
- Update changelog. [Martin Raspaud]
- Bump version: 0.1.0 → 0.1.1. [Martin Raspaud]
- Add imp to import list. [Martin Raspaud]


v0.1.0 (2017-11-13)
-------------------
- Update changelog. [Martin Raspaud]
- Bump version: 0.0.1 → 0.1.0. [Martin Raspaud]
- Add housekeeping config. [Martin Raspaud]
- Add a version file. [Martin Raspaud]
- Rename to trollmoves. [Martin Raspaud]
- Fix typo in servers log format. [Lars Orum Rasmussen]

  Added 'name' to mirrors log format

- Added 'name' to logging format. [Lars Orum Rasmussen]
- Added 'name' to logging format. [Lars Orum Rasmussen]
- Fix of harmfull behavior of the heartbeat monitor in case of bursts of
  req for push messages. [Lars Orum Rasmussen]
- Fixed (a fine) typo. [Lars Orum Rasmussen]
- Merge pull request #7 from pytroll/feature-heartbeat-monitor. [Martin
  Raspaud]

  Introducing heartbeat monitor
- Introducing heartbeat monitor. [Lars Orum Rasmussen]
- Merge pull request #6 from pytroll/issue-sftp. [Martin Raspaud]

  Issue sftp
- Better log messages for send/recv timeout. [ras]
- Cosmetic. [ras]
- Testing req timeout from config file. [ras]
- Added a sleep in PushRequester, good for big file transfers. [ras]
- Testing timeout III. [ras]
- Testing timeout II. [ras]
- Passing ssh key file from attributes. [ras]

  Testing timeout

- Specify key file, mainly for testing at Lannion. [ras]
- SFTP option, first go. [ras]
- Ignore TypeError exception from parser. [ras]
- Merge pull request #5 from pytroll/issue-heartbeat. [Martin Raspaud]

  Handle heartbeat
- A little more info in the clients published message. [Lars Orum
  Rasmussen]
- Merge branch 'develop' into issue-heartbeat. [Lars Orum Rasmussen]
- Fix dir creation not to include filename. [Martin Raspaud]
- Handle heartbeat. [Lars Orum Rasmussen]

  A few PEP8 issues

  StatCollector.collect, less knowledge of receive message

- Merge pull request #4 from pytroll/issue-pong. [Martin Raspaud]

  Defining self._station, used by pong
- Defining self._station, used by pong. [Lars Orum Rasmussen]
- Add heartbeating to servers. [Martin Raspaud]
- Merge pull request #3 from pytroll/desthost-not-localhost. [Martin
  Raspaud]

  Handling case where destination host (ftp server) is not local host
- Handling case where destination host (ftp server) is not local host.
  [Lars Orum Rasmussen]
- Exit cleanly from remove_it when something goes bad. [Martin Raspaud]
- Put scp/paramiko import in the function for bkwards compatibility.
  [Martin Raspaud]
- Add paramiko as a dependency. [Martin Raspaud]

  Since it is imported explicitely in the code, it has to be explicit in
  the requirements too.
- Merge pull request #1 from TAlonglong/develop. [Martin Raspaud]

  fixed conflict when adding scp ad option
- Fixed conflict when adding scp ad option. [Trygve Aspenes aka
  msg_xrus]
- Avoid using same inproc socket in each thread. [Martin Raspaud]
- Use inproc socket to avoid multithread tcp socket access. [Martin
  Raspaud]

  Multithread socket access seems to create problems, so instead push the
  messages back to the main thread.
- Add locking around request reception. [Martin Raspaud]
- Fix bug that Timer takes in a list of args. [Martin Raspaud]
- Allow adding delay to the move_it mirroring. [Martin Raspaud]
- Chmod the destination directory to 777. [Martin Raspaud]
- Ensure the destination dir exists before requesting a push. [Martin
  Raspaud]
- Fix minor bugs. [Martin Raspaud]
- Add locking when sending a message (for threaded cases) [Martin
  Raspaud]
- Fix mirroring. [Martin Raspaud]
- Allow specifying the destination server in the client part. [Martin
  Raspaud]
- Merge branch 'master' of github.com:pytroll/pytroll-file-utils into
  develop. [Martin Raspaud]

  Conflicts:
  	bin/remove_it.py
- Fix bug. [Adam.Dybbroe]

  It should now be more robust against situations where files/paths
  are removed from the applications themselves coincident when the
  clean script is being run

- Allow request_address to be provided in config file. [Martin Raspaud]
- Fix missing comma. [Martin Raspaud]
- Add pytroll mirror to the list of scripts. [Martin Raspaud]
- Fix outbound message metadata. [Martin Raspaud]
- Add mirroring capabilities. [Martin Raspaud]
- Try to reconnect to the move_it server in case of invalid message.
  [Martin Raspaud]

  It happened that the listener thread crashed because of a Message error
  ('This is not a 'pytroll:/' message (wrong magick word)'!')
  This patch addresses this.
- Allow aliases to be passed to client. [Martin Raspaud]
- Change final messages in move_it_client from push to file. [Martin
  Raspaud]
- Use router instead of rep to allow parallel request processing.
  [Martin Raspaud]
- Increase client file cache size to 11000. [Martin Raspaud]

  That will accomodate for example 24 hours of 0 degree service data.
- Detect pattern problems in the server. [Martin Raspaud]
- Improve file deletion not to crash on missing file. [Martin Raspaud]
- Enhance file deletion when required. [Martin Raspaud]
- Fix old file processing. [Martin Raspaud]
- Replace touching bith message sending. [Martin Raspaud]
- Do not treat __ files differently. [Martin Raspaud]
- Add missing pyzmq dependency. [Martin Raspaud]
- Implement file deletion in move_it_server. [Martin Raspaud]
- Add stat mode in move_it_client. [Martin Raspaud]
- Remove unneeded debug message. [Martin Raspaud]
- Add reload via SIGHUP in both client and server. [Martin Raspaud]
- Fix destination. [Martin Raspaud]
- Add a debug message. [Martin Raspaud]
- Corrct ftp path. [Martin Raspaud]
- Increase rubustness. [Martin Raspaud]
- First alpha version of the distributed move_it. [Martin Raspaud]
- Use mtime for file timestamps (remove-it) [Martin Raspaud]
- Do not try to remove non-empty dirs. [Martin Raspaud]
- Netifaces is needed. [Martin Raspaud]
- Add move_it client and server to list of scripts. [Martin Raspaud]
- Add client/sever operations for move_it. [Martin Raspaud]
- Add mailing possibility in remove_it and log traceback. [Martin
  Raspaud]
- Don't allow years and months in timedelta. [Martin Raspaud]
- Allow providing several -c args to remove-it. [Martin Raspaud]
- Fix log formatting. [Martin Raspaud]
- Bugfix. [Martin Raspaud]
- Add logging.handlers to import list. [Martin Raspaud]
- Bugfix. [Martin Raspaud]
- Bugfix. [Martin Raspaud]
- Initial commit. [Martin Raspaud]
- Initial commit. [Martin Raspaud]




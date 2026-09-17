"""Client for Trollmoves.

Moving and unpacking files
==========================

This program is comprised of two parts: this script and the configuration file.

The usage of this script is quite straightforward, just call it with the name
of the configuration file as argument.

The configuration file is comprised of sections describing the chain of
moving/unpacking.

Installation
------------

This scripts needs pyinotify, argparse, posttroll and trollsift which are available on pypi, or via pip/easy_install
(on redhat systems, install the packages python-inotify.noarch and python-argparse.noarch).  Other than this,
the script doesn't need any installation, and can be run as is. If you wish though, you can install it to your
standard python path with::

  python setup.py install


Configuration file
------------------

For example::

  [eumetcast_hrit]
  providers=tellicast_server:9090
  destination=/the/directory/you/want/stuff/in /another/directory/you/want/stuff/in
  login=username:greatPassword
  topic=/1b/hrit/zds
  publish_port=0

* 'provider' is the address of the server receiving the data you want.

* 'destinations' is the list of places to put the unpacked data in.

* 'topic' gives the topic to listen to on the provider side.

* 'publish_port' defines on which port to publish incomming files. 0 means random port.

Logging
-------

The logging is done on stdout per default. It is however possible to specify a logging config file with the -c
or --log-config option::

  move_it_client --log-config /path/to/mylogconfig.yaml myconfig.ini

"""

# TODO: implement ping and server selection

from trollmoves.client import MoveItClient, parse_args
from trollmoves.logging import setup_logging


def main():
    """Run the Trollmoves Client."""
    cmd_args = parse_args()
    logger = setup_logging("move_it_client", cmd_args)
    client = MoveItClient(cmd_args)

    try:
        client.reload_cfg_file(cmd_args.config_file)
        client.run()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    except Exception as err:
        logger.exception(err)
    finally:
        if client.running:
            client.chains_stop()


if __name__ == "__main__":
    main()

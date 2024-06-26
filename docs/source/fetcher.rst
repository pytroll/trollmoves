Pytroll fetcher
---------------

The pytroll-fetcher script and trollmoves.fetcher library are tools to fetch (download) files or objects from local or
remote filesystems.

Command line script
*******************

The pytroll-fetcher script listen to posttroll messages and reacts by downloading files to a given locations. The help
command of the script gives::

    usage: pytroll-fetcher [-h] [-c LOG_CONFIG] config

    Fetches files/objects advertised as messages.

    positional arguments:
    config                The yaml config file.

    options:
    -h, --help            show this help message and exit
    -c LOG_CONFIG, --log-config LOG_CONFIG
                            Log config file to use instead of the standard logging.

    Thanks for using pytroll-fetcher!

An example config would be for this command line interface would be::

    destination: /home/myuser/downloads/
    publisher_config:
        nameservers: false
        port: 1979
    subscriber_config:
        addresses:
        - ipc://some_pipe
        nameserver: false


The `publisher_config` and `subscriber_config` dictionaries in the config are passed straight to posttroll's
:py:func:`~posttroll.publisher.create_publisher_from_dict_config` and
:py:func:`~posttroll.subscriber.create_subscriber_from_dict_config` respectively, so refer to the corresponding
documentation to see how to populate them.


API
***

.. automodule:: trollmoves.fetcher
   :members:

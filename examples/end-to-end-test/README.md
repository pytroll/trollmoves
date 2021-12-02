# Running an end-to-end test for Trollmoves scripts

This walkthrough demonstrates how to connect and test Trollmoves systems to each other.

It is assumed that Trollmoves and all the required packages have been installed.

## Start a nameserver
Run the following command in a terminal:

    nameserver --no-multicast

We are disabling multicast to reduce the noise in the network.

## Start a Trollmoves Server

First, in a new terminal, create an input directory to use:

    mkdir /tmp/server_input_directory

Start the Server process using the supplied configuration file:

    move_it_server.py -v --disable-backlog -p 9011 server.ini

In another terminal, create a file in the input directory:

    echo "Unladen swallow" > /tmp/server_input_directory/test1.txt

This should trigger the Server to publish a message.

## Start a Trollmoves Mirror

reate a new directory for storing the mirrored files temporarily:

    mkdir /tmp/mirror_temporary_directory

Start the Mirror using the supplied configuration file:

    move_it_mirror.py -p 9012 -v mirror.ini

In a new terminal, create another test file to the Server input directory:

    echo "African swallow" > /tmp/server_input_directory/test2.txt

This should show that the Mirror got a message and re-published it.

## Start a Trollmoves Client

Create a target directory for the Client:

    mkdir /tmp/client_target_directory

Start the Client using the supplied configuration file:

    move_it_client.py -v client.ini

In a new terminal, create another test file:

    echo "European swallow" > /tmp/server_input_directory/test3.txt

This should trigger Server and eventually Mirror to notify Client that there is a new file,
Client to make a request to Mirror, which requests the file from Server and forwards it to
the target directory of the Client.

## Forwarding the data with Dispatcher

Create a target directory for Dispatcher:

    mkdir /tmp/dispatcher_target_directory

Start the Dispatcher:

    dispatcher.py -v dispatch.yaml

In a new terminal, create yet another test file for the Server to host:

    echo "Coconuts migrate?" > /tmp/input_directory/test4.txt

In the end the file should be in the following directories:

    /tmp/server_input_directory
    /tmp/client_target_directory
    /tmp/dispatcher_target_directory

and exist temporarily also in:

    /tmp/mirror_temporary_directory

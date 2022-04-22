Feature: Move it server and client
    Move it server and client interaction.

    Scenario: Simple file transfer
        Given We have a source directory
        And We have a separate destination directory
        And Move it server is started
        And Move it client is started

        When A new file arrives matching the pattern

        Then The file should be moved to the destination directory

    Scenario: Simple file publishing
        Given We have a source directory
        And A posttroll subscriber is started
        And Move it server with no request port is started

        When A new file arrives matching the pattern

        Then A posttroll message with filesystem information should be issued by the server

    Scenario: Simple file publishing with untarring
        Given We have a source directory
        And A posttroll subscriber is started
        And Move it server with no request port is started with untarring option activated

        When A new tar file arrives matching the pattern

        Then A posttroll message with filesystem information and untarred file collection should be issued by the server

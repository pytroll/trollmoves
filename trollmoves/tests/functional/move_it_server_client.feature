Feature: Move it server and client
    Move it server and client interaction.

    Scenario: Simple file transfer
        Given We have a source directory
        And We have a separate destination directory
        And Move it server is started
        And Move it client is started

        When A new file arrives matching the pattern

        Then The file should be moved to the destination directory

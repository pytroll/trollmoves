"""Module defining hooks for monitoring."""


class DummyHook():
    """Dummy hook class."""

    def error(self, msg):
        """Send an error."""
        pass

    def warning(self, msg):
        """Send a warning."""
        pass

    def ok(self, msg):
        """Send an ok."""
        pass

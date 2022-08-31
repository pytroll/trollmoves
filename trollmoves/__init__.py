"""The Trollmoves package."""

from .version import get_versions
__version__ = get_versions()['version']
del get_versions

FALSY = ["", "False", "false", "0", "off", "no"]
TRUTHY = ["True", "true", "on", "1", "yes"]

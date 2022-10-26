#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Move it Mirror."""

import logging.handlers

from trollmoves.mirror import MoveItMirror, parse_args

LOGGER = logging.getLogger("move_it_mirror")
LOG_FORMAT = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"


def main():
    """Start the mirroring."""
    cmd_args = parse_args()
    mirror = MoveItMirror(cmd_args)

    try:
        mirror.reload_cfg_file(cmd_args.config_file)
        mirror.run()
    except KeyboardInterrupt:
        LOGGER.debug("Interrupting")
    finally:
        if mirror.running:
            mirror.chains_stop()


if __name__ == '__main__':
    main()

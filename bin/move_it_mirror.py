"""Move it Mirror."""
from trollmoves.logging import setup_logging
from trollmoves.mirror import MoveItMirror, parse_args


def main():
    """Start the mirroring."""
    cmd_args = parse_args()
    logger = setup_logging("move_it_mirror", cmd_args)
    mirror = MoveItMirror(cmd_args)

    try:
        mirror.reload_cfg_file(cmd_args.config_file)
        mirror.run()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    finally:
        if mirror.running:
            mirror.chains_stop()


if __name__ == "__main__":
    main()

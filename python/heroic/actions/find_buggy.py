import logging
import base64

log = logging.getLogger(__name__)


def action(ns):
    """
    Finds buggy data_point row keys and writes them to file.
    """

    kill_path = ns.output_file.format(ns)

    log.info("Writing to {}".format(kill_path))

    buggy_count = 0

    with open(kill_path, "w") as kill:
        with ns.clusters(ns) as dao:
            for row, key in dao.list_keys(limit=ns.limit):
                if key.is_buggy():
                    log.info("buggy: {}".format(repr(key)))
                    buggy_count += 1
                    kill.write(base64.b64encode(row.key) + "\n")

                kill.flush()

    log.info("Found {} buggy row(s)".format(buggy_count))

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "find-buggy",
        help=action.__doc__)

    parser.add_argument("cluster", help="Cluster to search for buggy rows.")

    parser.add_argument(
        "-f", default="buggy_rows.{0.cluster}.txt",
        dest="output_file",
        metavar="<file>",
        help="Output file to write buggy rows to.")

    parser.set_defaults(action=action)

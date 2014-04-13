import itertools
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
            queries = []

            if not ns.exclude_data_points:
                queries.append(dao.list_data_point_row_keys(limit=ns.limit))

            if not ns.exclude_row_key_index:
                queries.append(dao.list_row_key_index_keys(limit=ns.limit))

            for i, (raw, key) in enumerate(itertools.chain(*queries)):
                if i % 1000 == 0:
                    log.info("Read: {}".format(i))

                if key.is_buggy():
                    log.info("buggy: {}".format(repr(key)))
                    buggy_count += 1
                    kill.write(base64.b64encode(raw) + "\n")

                kill.flush()

    log.info("Found {} buggy row(s)".format(buggy_count))

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "find-buggy",
        help=action.__doc__)

    parser.add_argument("cluster", help="Cluster to search for buggy rows.")

    parser.add_argument("--exclude-data-points",
                        action="store_const", const=True, default=False,
                        help=(
                            "Exclude data_points column family when looking "
                            "for keys."
                        ))
    parser.add_argument("--exclude-row-key-index",
                        action="store_const", const=True, default=False,
                        help=(
                            "Exclude row_key_index column family when looking "
                            "for keys."
                        ))
    parser.add_argument(
        "-f", default="buggy_rows.{0.cluster}.txt",
        dest="output_file",
        metavar="<file>",
        help="Output file to write buggy rows to.")

    parser.set_defaults(action=action)

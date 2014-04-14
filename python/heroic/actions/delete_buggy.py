import logging
import base64

from heroic.models import RowKey

log = logging.getLogger(__name__)


def read_rows(path):
    with open(path, "r") as kill:
        for line in kill:
            key = base64.b64decode(line)
            yield RowKey.deserialize(key)


def action(ns):
    """
    Finds buggy data_point row keys and writes them to file.
    """

    with ns.clusters(ns) as dao:
        kill_path = ns.input_file.format(ns)

        log.info("Reading from {}".format(kill_path))

        rows = list(read_rows(kill_path))
        length = len(rows)

        for i, row_key in enumerate(rows):
            if not row_key.is_buggy():
                log.warn("KEY NOT BUGGY, SKIPPING: {}".format(
                    repr(row_key)))
                continue

            if ns.pretend:
                log.info("Would delete ({}/{}): {}".format(
                    i, length, repr(row_key)))
                continue

            log.info("Deleting ({}/{}): {}".format(
                i, length, repr(row_key)))

            if not ns.exclude_data_points:
                dao.delete_data_points(row_key)

            if not ns.exclude_row_key_index:
                dao.delete_row_key_index(row_key)

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "delete-buggy",
        help=action.__doc__)

    parser.add_argument("cluster", help="Cluster where to delete buggy rows.")

    parser.add_argument("--pretend",
                        action="store_const", const=True, default=False,
                        help=(
                            "Only pretend to delete rows and print the ones "
                            "which would be deleted."
                        ))
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
        dest="input_file",
        metavar="<file>",
        help="Input file to read rows to delete from.")

    parser.set_defaults(action=action)

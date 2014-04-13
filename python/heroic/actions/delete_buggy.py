import logging
import base64

from heroic.models import RowKey

log = logging.getLogger(__name__)


DELETE_DATA_POINTS = "DELETE FROM data_points WHERE key = ?"
DELETE_ROW_KEY_INDEX = (
    "DELETE FROM row_key_index "
    "WHERE key=? AND column1 = ?")


def action(ns):
    """
    Finds buggy data_point row keys and writes them to file.
    """

    deleted_count = 0

    with ns.clusters(ns) as session:
        kill_path = ns.input_file.format(ns)

        log.info("Reading from {}".format(kill_path))

        try:
            with open(kill_path, "r") as kill:
                for line in kill:
                    key = base64.b64decode(line)
                    row_key = RowKey.deserialize(key)

                    if not row_key.is_buggy():
                        log.warn("KEY NOT BUGGY, SKIPPING: {}".format(
                            repr(row_key)))
                        continue

                    row_key_index_stmt = session.prepare(
                        DELETE_ROW_KEY_INDEX).bind((row_key.key, key,))
                    data_points_stmt = session.prepare(
                        DELETE_DATA_POINTS).bind((key,))

                    log.info("Deleting: {}".format(repr(row_key)))

                    result0 = session.execute(row_key_index_stmt, timeout=None)
                    result1 = session.execute(data_points_stmt, timeout=None)
                    log.info("result0={}".format(repr(result0)))
                    log.info("result1={}".format(repr(result1)))
                    deleted_count += 1
        finally:
            log.info("Deleted {} buggy row(s)".format(deleted_count))

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "delete-buggy",
        help=action.__doc__)

    parser.add_argument("cluster", help="Cluster where to delete buggy rows.")

    parser.add_argument(
        "-f", default="buggy_rows.{0.cluster}.txt",
        dest="input_file",
        metavar="<file>",
        help="Input file to read rows to delete from.")

    parser.set_defaults(action=action)

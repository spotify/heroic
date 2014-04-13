import logging
import base64

from heroic.models import RowKey

log = logging.getLogger(__name__)


def action(ns):
    """
    Finds buggy data_point row keys and writes them to file.
    """

    deleted_count = 0

    with ns.clusters(ns) as dao:
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

                    log.info("Deleting: {}".format(repr(row_key)))
                    result0, result1 = dao.delete_timeseries(key)

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

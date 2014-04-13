import logging
import base64
import itertools

from heroic.models import RowKey

log = logging.getLogger(__name__)


SELECT_STMT = (
    "SELECT DISTINCT key FROM data_points "
    "WHERE token(key) > token(?) "
    "LIMIT ?")


def action(ns):
    """
    Finds buggy data_point row keys and writes them to file.
    """

    with ns.clusters(ns) as session:
        kill_path = ns.output_file.format(ns)

        log.info("Writing to {}".format(kill_path))

        buggy_count = 0

        last = ""

        with open(kill_path, "w") as kill:
            for i in itertools.count():
                if last is None:
                    break

                start = i * ns.limit
                stop = (i + 1) * ns.limit

                log.info("Scanning from: {} - {}".format(start, stop))

                stmt = session.prepare(SELECT_STMT).bind((last, ns.limit))

                result = session.execute(stmt, timeout=None)

                last = None

                for row in result:
                    key = RowKey.deserialize(row.key)

                    if key.is_buggy():
                        log.info("buggy: {}".format(repr(key)))
                        buggy_count += 1
                        kill.write(base64.b64encode(row.key) + "\n")

                    last = row.key

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

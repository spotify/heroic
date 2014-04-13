import logging

log = logging.getLogger(__name__)


def action(ns):
    """
    List all keys in a cluster from the data_points table.
    """

    with ns.clusters(ns) as dao:
        for row, key in dao.list_keys(limit=ns.limit):
            log.info(repr(key))

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "list-keys",
        help=action.__doc__)

    parser.add_argument("cluster", help="Cluster to list keys in.")

    parser.set_defaults(action=action)

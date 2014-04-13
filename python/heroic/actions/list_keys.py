import itertools
import logging

log = logging.getLogger(__name__)


def action(ns):
    """
    List all keys in a cluster from the data_points table.
    """

    with ns.clusters(ns) as dao:
        queries = []

        if not ns.exclude_data_points:
            queries.append(dao.list_data_point_row_keys(limit=ns.limit))

        if not ns.exclude_row_key_index:
            queries.append(dao.list_row_key_index_keys(limit=ns.limit))

        for row, key in itertools.chain(*queries):
            log.info(repr(key))

    return 0


def setup(subparsers):
    parser = subparsers.add_parser(
        "list-keys",
        help=action.__doc__)

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
    parser.add_argument("cluster", help="Cluster to list keys in.")

    parser.set_defaults(action=action)

"""

"""

import itertools
import argparse
import sys
import contextlib

from heroic.models import RowKey


def find_buggy_rows(subparsers):
    """
    Finds buggy rows and writes them to file.
    """
    import base64

    def action(ns):
        with ns.clusters(ns) as session:
            last_key = None

            with open(ns.output_file, "w") as kill:
                for i in itertools.count():
                    if last_key is not None:
                        where_stmt = session.prepare(
                            "SELECT DISTINCT key FROM data_points "
                            "WHERE token(key) > token(?) LIMIT ?")
                        stmt = where_stmt.bind((last_key, ns.limit))
                    else:
                        stmt = session.prepare(
                            "SELECT DISTINCT key FROM data_points LIMIT ?"
                        ).bind((ns.limit,))

                    empty = True
                    result = session.execute(stmt, timeout=None)

                    for row in result:
                        empty = False

                        last_key = row
                        key = RowKey.deserialize(row.key)

                        if key.is_buggy():
                            print "DIE:", key.key, key.timestamp, key.tags
                            kill.write(base64.b64encode(row.key) + "\n")

                    kill.flush()

                    print "DONE: {} - {}".format(
                        i * ns.limit, (i + 1) * ns.limit)

                    if empty:
                        break

        return 0

    parser = subparsers.add_parser(
        "find-buggy-rows",
        help=find_buggy_rows.__doc__)

    parser.add_argument("cluster", help="Cluster to search for buggy rows.")

    parser.add_argument(
        "-f", default="buggy_rows.txt",
        dest="output_file",
        metavar="<file>",
        help="Output file to write buggy rows to.")

    parser.set_defaults(action=action)


ACTIONS = [
    find_buggy_rows
]


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", dest="config_path", metavar="<file>",
                        default="heroic.yaml",
                        help="Configuration file to use.")
    parser.add_argument("-k", dest="keyspace", metavar="<keyspace>",
                        default="kairosdb", help="Cassandra keyspace to use.")
    parser.add_argument("-l", dest="limit", metavar="<limit>",
                        default=100,
                        help="Max number of rows to query at-a-time.")

    subparsers = parser.add_subparsers()

    for a in ACTIONS:
        a(subparsers)

    return parser


def setup_config(path):
    import yaml

    with open(path) as f:
        return yaml.load(f)


def prepare_cluster(seeds):
    from cassandra.cluster import Cluster

    @contextlib.contextmanager
    def connect(ns):
        print "Connecting..."
        cluster = Cluster(seeds, control_connection_timeout=None)
        session = cluster.connect()
        session.execute("USE {}".format(ns.keyspace))

        print "Got session"

        try:
            yield session
        except:
            cluster.shutdown()
            raise

    return connect


def prepare_clusters(config):
    setup = dict()

    for name, seeds in config["clusters"].items():
        setup[name] = prepare_cluster(seeds)

    def clusters(ns):
        return setup[ns.cluster](ns)

    return clusters


def main(args):
    parser = setup_parser()
    ns = parser.parse_args(args)

    config = setup_config(ns.config_path)
    ns.clusters = prepare_clusters(config)

    return ns.action(ns)


def entry():
    sys.exit(main(sys.argv[1:]))

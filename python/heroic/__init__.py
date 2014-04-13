import logging
import argparse
import sys
import contextlib

from heroic.actions import find_buggy
from heroic.actions import delete_buggy

log = logging.getLogger(__name__)


ACTIONS = [
    find_buggy.setup,
    delete_buggy.setup
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
                        type=int,
                        help="Max number of rows to query at-a-time.")

    subparsers = parser.add_subparsers()

    for setup in ACTIONS:
        setup(subparsers)

    return parser


def setup_config(path):
    import yaml

    with open(path) as f:
        return yaml.load(f)


def prepare_cluster(seeds):
    from cassandra.cluster import Cluster

    @contextlib.contextmanager
    def connect(ns):
        log.info("Connecting...")
        cluster = Cluster(seeds, control_connection_timeout=None)
        session = cluster.connect()
        session.execute("USE {}".format(ns.keyspace))

        log.info("Connected!")

        try:
            yield session
        except:
            cluster.shutdown()
            raise

    return connect


def prepare_clusters(config):
    prepared = dict()

    for name, seeds in config["clusters"].items():
        prepared[name] = prepare_cluster(seeds)

    def clusters(ns):
        setup = prepared.get(ns.cluster)

        if setup is None:
            raise Exception("No such cluster: {}".format(ns.cluster))

        return setup(ns)

    return clusters


def main(args):
    logging.basicConfig(level=logging.INFO)
    parser = setup_parser()
    ns = parser.parse_args(args)

    config = setup_config(ns.config_path)
    ns.clusters = prepare_clusters(config)

    return ns.action(ns)


def entry():
    sys.exit(main(sys.argv[1:]))

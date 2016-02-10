import unittest
import json
import time

import tests.helpers as h

FEATURES = ['com.spotify.heroic.distributed_aggregations']


class TestCluster(unittest.TestCase):
    # @h.timeout(60)
    def test_cluster_query(self):
        with h.managed_cluster(1, features=FEATURES) as (a,):
            # write some data into each shard
            w1 = a.write({"key": "test", "tags": {}},
                         {"type": "points", "data": [[1000, 1], [2000, 2]]})

            time.sleep(5)

            # query for the data (without aggregation)
            result = a.query_metrics(
                {"query": "* from points(1000, 2000) where $key = test"})

            self.assertEquals(200, result.status_code)

            result = result.json()

            self.assertEquals(0, len(result['errors']))
            self.assertEquals(1, len(result['result']))

            values = result['result'][0]['values']

            self.assertEquals([[1000, 1.0], [2000, 2.0]], list(values))

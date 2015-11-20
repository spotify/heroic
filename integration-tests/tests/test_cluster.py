import unittest
import json
import time

import tests.helpers as h


class TestCluster(unittest.TestCase):
    # @h.timeout(60)
    def test_cluster_query(self):
        config_a = {"cluster":
                    {"protocols": [{"type": "nativerpc", "port": 2000}],
                     "useLocal": True, "tags": {"foo": "bar"}}}
        config_b = {"cluster":
                    {"protocols": [{"type": "nativerpc", "port": 2001}],
                     "useLocal": True, "tags": {"foo": "bar2"}}}

        with h.managed(config_a, config_b, args=["-P", "memory"]) as (a, b):
            status_a = a.status()
            self.assertEquals(True, status_a["ok"], status_a)
            a.cluster_add_node('nativerpc://localhost:2001')
            cluster_status_a = a.cluster_status()
            self.assertEquals(2, len(a.cluster_status()["nodes"]),
                              cluster_status_a)

            status_b = b.status()
            self.assertEquals(True, status_b["ok"], status_b)
            b.cluster_add_node('nativerpc://localhost:2000')
            cluster_status_b = b.cluster_status()
            self.assertEquals(2, len(b.cluster_status()["nodes"]),
                              cluster_status_b)

            # write some data into each shard
            w1 = a.write({"key": "test", "tags": {}},
                         {"type": "points", "data": [[1000, 1], [2000, 2]]})
            #w2 = b.write({"key": "test", "tags": {}},
            #             {"type": "points", "data": [[3000, 3], [4000, 4]]})

            self.assertEquals(200, w1.status_code)
            #self.assertEquals(200, w2.status_code)

            time.sleep(5)

            # query for the data (without aggregation)
            result = a.query_metrics(
                {"query": "* from points(1000, 2000) where $key = test", "options": {"tracing": True}})

            self.assertEquals(200, result.status_code)

            result = result.json()

            print json.dumps(result)

            self.assertEquals(0, len(result['errors']))
            self.assertEquals(1, len(result['result']))

            values = result['result'][0]['values']

            self.assertEquals(
                [[1000, 1.0], [2000, 2.0], [3000, 3.0], [4000, 4.0]],
                list(sorted(values)))

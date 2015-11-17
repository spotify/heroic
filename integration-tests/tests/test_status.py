import unittest

import tests.helpers as h


class TestStatus(unittest.TestCase):
    @h.timeout(60)
    def test_cluster_status(self):
        config_a = {"cluster": {"protocols": [{"type": "nativerpc", "port": 2000}],
                                "useLocal": False, "tags": {"foo": "bar"}}}
        config_b = {"cluster": {"protocols": [{"type": "nativerpc", "port": 2001}],
                                "useLocal": False, "tags": {"foo": "bar2"}}}

        with h.managed(config_a, config_b) as (a, b):
            status_a = a.status()
            self.assertEquals(True, status_a["ok"], status_a)
            a.cluster_add_node('nativerpc://localhost:2001')
            cluster_status_a = a.cluster_status()
            self.assertEquals(1, len(a.cluster_status()["nodes"]),
                              cluster_status_a)

            status_b = b.status()
            self.assertEquals(True, status_b["ok"], status_b)
            b.cluster_add_node('nativerpc://localhost:2000')
            cluster_status_b = b.cluster_status()
            self.assertEquals(1, len(b.cluster_status()["nodes"]),
                              cluster_status_b)

package com.spotify.heroic.backend;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.statistics.BackendManagerReporter;

public class ListBackendManagerTest {
    @Mock
    private List<MetricBackend> metricBackends;
    @Mock
    private List<EventBackend> eventBackends;
    @Mock
    private AggregationCache cache;
    @Mock
    private BackendManagerReporter reporter;

    private static final long MAGNITUDE = 42;

    private ListBackendManager manager;

    @Before
    public void before() {
        this.manager = new ListBackendManager(metricBackends, eventBackends, cache, reporter, MAGNITUDE);
    }

    @Test(expected=QueryException.class)
    public void testQueryMustBeDefined() throws QueryException {
        manager.queryMetrics(null);
    }
}

package com.spotify.heroic.metric;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.heroic.common.GroupSet;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import com.spotify.heroic.statistics.MetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/*
 * Start of a test for LocalMetricManager
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalMetricManagerTest {
    private LocalMetricManager manager;

    @Mock
    private AsyncFramework async;

    @Mock
    private MetadataManager metadataManager;

    @Mock
    private MetricBackendReporter reporter;

    @Mock
    private MetadataManager metadata;

    @Mock
    private MetricBackend metricBackend;

    @Mock
    private AsyncFuture<FetchData> fetchDataFuture;

    @Before
    public void setup() {
        final OptionalLimit groupLimit = OptionalLimit.empty();
        final OptionalLimit seriesLimit = OptionalLimit.empty();
        final OptionalLimit aggregationLimit = OptionalLimit.empty();
        final OptionalLimit dataLimit = OptionalLimit.empty();
        final int fetchParallelism = 20;
        final boolean failOnLimits = true;
        final Groups groups = Groups.of("foo");
        doReturn(groups).when(metricBackend).groups();
        final GroupSet<MetricBackend> groupSet =
            GroupSet.build(Collections.singletonList(metricBackend), Optional.empty());

        final QueryLogger queryLogger = mock(QueryLogger.class);
        final QueryLoggerFactory queryLoggerFactory = mock(QueryLoggerFactory.class);
        when(queryLoggerFactory.create(any())).thenReturn(queryLogger);

        manager = new LocalMetricManager(groupLimit, seriesLimit, aggregationLimit, dataLimit,
            fetchParallelism, failOnLimits, async, groupSet, metadata, reporter,
            queryLoggerFactory);
    }

    @Test
    public void testUseDefaultBackend() {
        assertNotNull(manager.useDefaultGroup());
    }
}

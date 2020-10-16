package com.spotify.heroic.metric;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.heroic.common.GroupSet;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.LocalMetricManager.QuotaWatcher;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.noop.NoopMetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Start of a test for LocalMetricManager
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalMetricManagerTest {

    private static final Logger log = LoggerFactory.getLogger(LocalMetricManagerTest.class);

    public static final int NUM_WATCHERS = 10_000;
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
        final OptionalLimit concurrentQueriesBackoff = OptionalLimit.empty();
        final int fetchParallelism = 20;
        final boolean failOnLimits = true;
        final Groups groups = new Groups("foo");
        doReturn(groups).when(metricBackend).groups();
        final GroupSet<MetricBackend> groupSet =
            GroupSet.build(Collections.singletonList(metricBackend), Optional.empty());

        final QueryLogger queryLogger = mock(QueryLogger.class);
        final QueryLoggerFactory queryLoggerFactory = mock(QueryLoggerFactory.class);
        when(queryLoggerFactory.create(any())).thenReturn(queryLogger);

        manager = new LocalMetricManager(groupLimit, seriesLimit, aggregationLimit, dataLimit,
            concurrentQueriesBackoff, fetchParallelism, failOnLimits, async, groupSet, metadata,
            reporter, queryLoggerFactory);
    }

    @Test
    public void testUseDefaultBackend() {
        assertNotNull(manager.useDefaultGroup());
    }

    @Test
    public void testHashCodeAsMapKeyFails() throws InterruptedException {
        final ConcurrentMap<QuotaWatcher, QuotaWatcher> correctWatcherMap = new ConcurrentHashMap<>();

        final int nThreads = Runtime.getRuntime().availableProcessors();

        var pool = Executors.newFixedThreadPool(nThreads);

        var dimr = NoopMetricBackendReporter.DATA_IN_MEMORY_REPORTER;

        var realWatchers = LocalMetricManager.getQuotaWatchers();

        var realClobberCount = new AtomicInteger();
        var correctClobberCount = new AtomicInteger();

        final Runnable hashMapFillerJob = () -> {
            System.out.println("Hi, I'm thread ID " + Thread.currentThread().getName() + ", and I'm starting up");
            for (int i = 0; i < NUM_WATCHERS; i++) {
                final QuotaWatcher qw = new QuotaWatcher(1000 + i, 2000 + i, dimr);

                // Count when the "correctly" implemented HashMap clobbers a QueryWatcher
                if (correctWatcherMap.containsKey(qw)) {
                    correctClobberCount.incrementAndGet();
                } else {
                    correctWatcherMap.put(qw, qw);
                }

                // Count when the "incorrectly" implemented HashMap clobbers a QueryWatcher
                final Integer idx = Integer.class.cast(i);
                if (realWatchers.containsKey(idx)) {
                    realClobberCount.incrementAndGet();
                } else {
                    realWatchers.put(idx, qw);
                }
            }
            System.out.println("Hi, I'm thread ID " + Thread.currentThread().getName() + ", and I'm done");
        };

        System.out.println("All threads are raring to go");

        for (int i = 0; i < nThreads; i++) {
            pool.execute(hashMapFillerJob);
        }

        System.out.println("All threads have launched. Now we wait...");

        // This is hacky, but surprisingly there isn't a nice, elegant way of awaiting
        // thread termination apart from converting the Runnables to a list of Futures,
        // and I can't be arsed, basically.
        pool.awaitTermination(4, TimeUnit.SECONDS);

        System.out.println("All threads are done");

        // We expect 0 clobbers on the "correct" impl and N (which is non-deterministic)
        // for the "incorrect" impl.
        assertEquals(0, correctClobberCount.get());
        assertTrue("Highly unexpected, there should be clobbered map entries.", realClobberCount.get() > 0);
    }
}

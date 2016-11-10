package com.spotify.heroic;

import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.grammar.QueryParser;
import eu.toolchain.async.AsyncFramework;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CoreQueryManagerTest {
    private CoreQueryManager manager;

    @Mock
    private AsyncFramework async;

    @Mock
    private ClusterManager cluster;

    @Mock
    private QueryParser parser;

    @Mock
    private QueryCache queryCache;

    @Mock
    private AggregationFactory aggregations;

    @Before
    public void setup() {
        manager =
            new CoreQueryManager(Features.empty(), async, cluster, parser, queryCache, aggregations,
                OptionalLimit.empty(), false, OptionalLimit.empty());
    }

    @Test
    public void testEndRangeIsNow() {
        final DateRange range = DateRange.create(50_000L, 150_000L);

        final DateRange shiftedRange = manager.buildShiftedRange(range, 5_000, 150_000L);

        assertEquals(DateRange.create(40_000L, 140_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsTooCloseToNow() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        final DateRange shiftedRange = manager.buildShiftedRange(range, 5_000, 154_000L);

        assertEquals(DateRange.create(40_000L, 140_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsOk() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        final DateRange shiftedRange = manager.buildShiftedRange(range, 5_000, 184_000L);

        assertEquals(DateRange.create(50_000L, 150_000L), shiftedRange);
    }

    @Test
    public void testEndRangeIsInTheFuture() {
        final DateRange range = DateRange.create(50_000L, 180_000L);

        final DateRange shiftedRange = manager.buildShiftedRange(range, 5_000, 150_000L);

        assertEquals(DateRange.create(10_000L, 140_000L), shiftedRange);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStartRangeIsInTheFuture() {
        final DateRange range = DateRange.create(50_000L, 153_000L);

        manager.buildShiftedRange(range, 5_000, 40_000L);
    }
}

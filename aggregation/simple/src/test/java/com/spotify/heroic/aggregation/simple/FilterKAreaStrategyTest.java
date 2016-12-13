package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.MetricCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterKAreaStrategyTest {

    @Test
    public void testNoDatapoints() {
        List<FilterableMetrics<Integer>> metrics = Arrays.asList(new FilterableMetrics(null,
            () -> MetricCollection.points(Collections.EMPTY_LIST)));
        FilterKAreaStrategy filter = new FilterKAreaStrategy(FilterKAreaType.BOTTOM, 1);

        // We expect timeseries with 0 datapoints to be filtered away
        assertEquals(filter.filter(metrics).size(), 0);

    }
}

/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class FilterPointsThresholdStrategyTest {
    private static final int POINT_RANGE_START = 0;
    private static final int TEST_THRESHOLD = 38;
    private static final int POINT_RANGE_END = 100;

    private MetricCollection initialMetrics;

    @Before
    public void setUp() throws Exception {
        initialMetrics = MetricCollection.build(MetricType.POINT, pointsRange());
    }

    @Test
    public void testFilterPointsAbove() throws Exception {
        FilterPointsThresholdStrategy strategy = new FilterPointsThresholdStrategy(FilterKThresholdType.ABOVE, TEST_THRESHOLD);
        List<Point> result = strategy.apply(initialMetrics).getDataAs(Point.class);
        assertFalse(result.stream().map(Point::getValue).anyMatch(v -> v <= TEST_THRESHOLD));
    }

    @Test
    public void testFilterPointsBelow() throws Exception {
        FilterPointsThresholdStrategy strategy = new FilterPointsThresholdStrategy(FilterKThresholdType.BELOW, TEST_THRESHOLD);
        List<Point> result = strategy.apply(initialMetrics).getDataAs(Point.class);
        assertFalse(result.stream().map(Point::getValue).anyMatch(v -> v >= TEST_THRESHOLD));
    }

    @Test
    public void testFilterDataIsDifferentFromPointsThenCollectionIsNotProcessed() throws Exception {
        FilterPointsThresholdStrategy strategy = new FilterPointsThresholdStrategy(FilterKThresholdType.BELOW, TEST_THRESHOLD);
        MetricCollection eventsCollection = MetricCollection.events(eventsRange());
        MetricCollection result = strategy.apply(eventsCollection);
        assertEquals(eventsCollection, result);
    }

    private List<Point> pointsRange() {
        return IntStream.range(POINT_RANGE_START, POINT_RANGE_END).mapToObj(i -> new Point(i, i)).collect(toList());
    }

    private List<Event> eventsRange() {
        return IntStream.range(POINT_RANGE_START, POINT_RANGE_END).mapToObj(Event::new).collect(toList());
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.*;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class PointsBelowInstanceTest {
    @Test
    public void testFilterPointsAboveKSession() {
        final GroupingAggregation g1 =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final AggregationInstance a1 = ChainInstance.of(g1, new PointsBelowInstance(2));

        final Set<Series> states = new HashSet<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "a"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "ash", "host", "b"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon", "host", "c"));

        states.add(s1);
        states.add(s2);
        states.add(s3);

        final AggregationSession session = a1.session(new DateRange(0, 10000));

        session.updatePoints(s1.getTags(), ImmutableSet.of(s1),
            ImmutableList.of(new Point(2, 2.0), new Point(3, 2.0)));
        session.updatePoints(s2.getTags(), ImmutableSet.of(s2),
            ImmutableList.of(new Point(2, 3.0), new Point(3, 3.0)));
        session.updatePoints(s3.getTags(), ImmutableSet.of(s3),
            ImmutableList.of(new Point(2, 1.0), new Point(3, 1.0)));

        final List<AggregationOutput> result = session.result().getResult();

        assertSeries(3, result);
        assertPoints(2, 2, result);
    }


    private void assertSeries(int series, List<AggregationOutput> result) {
        assertEquals(series, result.size());
    }

    private void assertPoints(int k, int expected, List<AggregationOutput> result) {
        assertFalse(result.stream().flatMap( s -> s.getMetrics().getDataAs(Point.class).stream()).map(Point::getValue).anyMatch(v -> v >= k));
        assertEquals(expected, result.stream().flatMap(s -> s.getMetrics().getDataAs(Point.class).stream()).count());
    }
}

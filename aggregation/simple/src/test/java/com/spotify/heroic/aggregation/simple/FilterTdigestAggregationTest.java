/*
 * Copyright (c) 2020 Spotify AB.
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

import static com.spotify.heroic.aggregation.simple.DistributionPointUtils.*;
import static org.junit.Assert.assertEquals;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.ChainInstance;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.GroupInstance;
import com.spotify.heroic.aggregation.GroupingAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class FilterTdigestAggregationTest {
   private static final  double [] data1 = {0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1};
   private static final  double [] data2 = {0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2};
   private static final  double [] data3 = {0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3};
   private static final long timestamp = 1;

    @Test
    public void testFilterKAreaSession() {
        final GroupingAggregation g =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        double [] quantiles = {0.5,0.75,0.99};

        final AggregationInstance b1 = ChainInstance.of(g, new TdigestStatInstance(1, 1, quantiles ));


        final Set<Series> series = new HashSet<>();
        final Series s1 = Series.of("foo", ImmutableMap.of("site", "s1", "host", "h1"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "s2" , "host", "h2"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "s3",  "host", "h3" ));

        series.add(s1);
        series.add(s2);
        series.add(s3);

        final AggregationSession session = b1.session(new DateRange(0, 10000));

        session.updateDistributionPoints(s1.getTags(), series,
            ImmutableList.of(createDistributionPoint(data1,timestamp),createDistributionPoint(data1,timestamp+ 1) ));
        session.updateDistributionPoints(s2.getTags(), series,
            ImmutableList.of(createDistributionPoint(data2,timestamp),createDistributionPoint(data2,timestamp + 1)));
        session.updateDistributionPoints(s3.getTags(), series,
            ImmutableList.of(createDistributionPoint(data3,timestamp), createDistributionPoint(data3,timestamp + 2)));

        final List<AggregationOutput> result = session.result().getResult();

        assertEquals(1, result.size());
    }
}

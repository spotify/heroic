/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.spotify.heroic.aggregation.simple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.metric.Point;

public class QuantileBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();
    private static final double ERROR = 0.01;

    @Test
    public void testCount() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.5, ERROR);
        b.updatePoint(TAGS, new Point(0, 1337.0));
        Assert.assertEquals(1337.0, b.value(), 0.0);
    }

    @Test
    public void testQuantiles() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.5, ERROR);

        for (int i = 1; i <= 10000; i++) {
            b.updatePoint(TAGS, new Point(0, i));
        }

        Assert.assertEquals(5000.0, b.value(), 10000 * ERROR);
    }

    @Test
    public void testQuantiles2() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.1, ERROR);

        for (int i = 1; i <= 10000; i++) {
            b.updatePoint(TAGS, new Point(0, i));
        }

        Assert.assertEquals(1000.0, b.value(), 10000 * ERROR);
    }
}

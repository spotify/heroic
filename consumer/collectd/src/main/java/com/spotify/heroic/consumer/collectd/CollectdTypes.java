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

package com.spotify.heroic.consumer.collectd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CollectdTypes {
    public static final String KEY = "collectd";

    private final Map<String, Mapping> mappings = new HashMap<>();

    public CollectdTypes() {
        mappings.put("cpu", new Mapping(ImmutableList.of(new CPU())));
        mappings.put("aggregation", new Mapping(ImmutableList.of(new Aggregation())));
    }

    public List<WriteMetric> convert(final CollectdSample sample, final Iterable<Map.Entry<String, String>> tags) {
        final Mapping mapping = mappings.get(sample.getPlugin());

        if (mapping == null) {
            // XXX: fallback to a worse strategy than explicit mapping?
            return ImmutableList.of();
        }

        return mapping.convert(sample, tags);
    }

    @Data
    public static class Mapping {
        private final List<Field> fields;

        public List<WriteMetric> convert(final CollectdSample sample, final Iterable<Map.Entry<String, String>> tags) {
            final long time = sample.getTime() * 1000;

            final Iterator<Field> fields = this.fields.iterator();
            final Iterator<CollectdValue> values = sample.getValues().iterator();

            final ImmutableList.Builder<WriteMetric> writes = ImmutableList.builder();

            while (fields.hasNext()) {
                if (!values.hasNext()) {
                    throw new IllegalArgumentException("too few values for mapping");
                }

                final Field field = fields.next();
                final CollectdValue value = values.next();

                final Series series = Series.of(KEY,
                        Iterables.concat(tags, field.tags(sample, value).entrySet()).iterator());
                final Point point = new Point(time, value.convert(field));

                final MetricCollection data = MetricCollection
                        .points(ImmutableList.of(point));

                writes.add(new WriteMetric(series, data));
            }

            return writes.build();
        }
    }

    @Data
    public static class Field {
        private final String name;
        private final double lower;
        private final double upper;

        public double convertCounter(long counter) {
            return Long.valueOf(counter).doubleValue();
        }

        public Map<String, String> tags(final CollectdSample s, final CollectdValue v) {
            return ImmutableMap.of();
        }

        public double convertGauge(double gauge) {
            return gauge;
        }

        public double convertDerive(long derived) {
            return Long.valueOf(derived).doubleValue();
        }

        public double convertAbsolute(long absolute) {
            return Long.valueOf(absolute).doubleValue();
        }
    }

    public static class CPU extends Field {
        public static final String CPU_ID = "cpu_id";
        public static final String WHAT_FORMAT = "cpu-jiffies-%s";

        public CPU() {
            super("value", 0D, Double.POSITIVE_INFINITY);
        }

        @Override
        public Map<String, String> tags(final CollectdSample s, final CollectdValue v) {
            final ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();

            tags.put("what", String.format(WHAT_FORMAT, s.getTypeInstance()));
            tags.put(CPU_ID, s.getPluginInstance());

            return tags.build();
        }
    }

    public static class Aggregation extends Field {
        public Aggregation() {
            super("value", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        }

        @Override
        public Map<String, String> tags(final CollectdSample s, final CollectdValue v) {
            final ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();

            tags.put("what", s.getPluginInstance());

            if (!"".equals(s.getType())) {
                tags.put("collectd_type", s.getType());
            }

            if (!"".equals(s.getTypeInstance())) {
                tags.put("collectd_type_instance", s.getTypeInstance());
            }

            return tags.build();
        }
    }
}
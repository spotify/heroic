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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;

import lombok.Data;

public class CollectdTypes {
    private static final Map<String, Mapping> mappings = new HashMap<>();

    {
        mappings.put("cpu", new Mapping(ImmutableList.of(new CPU())));
        mappings.put("aggregation", new Mapping(ImmutableList.of(new Aggregation())));
    }

    public static final String DEFAULT_KEY = "collectd";

    private static final String DEFAULT_PLUGIN_TAG = "plugin";
    private static final String DEFAULT_PLUGIN_INSTANCE_TAG = "plugin_instance";
    private static final String DEFAULT_TYPE_TAG = "type";
    private static final String DEFAULT_TYPE_INSTANCE_TAG = "type_instance";

    private final String key;
    private final String pluginTag;
    private final String pluginInstanceTag;
    private final String typeTag;
    private final String typeInstanceTag;

    @JsonCreator
    public CollectdTypes(@JsonProperty("key") Optional<String> key,
            @JsonProperty("pluginTag") Optional<String> pluginTag,
            @JsonProperty("pluginInstanceTag") Optional<String> pluginInstanceTag,
            @JsonProperty("typeTag") Optional<String> typeTag,
            @JsonProperty("typeInstanceTag") Optional<String> typeInstanceTag) {
        this.key = key.orElse(DEFAULT_KEY);
        this.pluginTag = pluginTag.orElse(DEFAULT_PLUGIN_TAG);
        this.pluginInstanceTag = pluginInstanceTag.orElse(DEFAULT_PLUGIN_INSTANCE_TAG);
        this.typeTag = typeTag.orElse(DEFAULT_TYPE_TAG);
        this.typeInstanceTag = typeInstanceTag.orElse(DEFAULT_TYPE_INSTANCE_TAG);
    }

    public static CollectdTypes supplyDefault() {
        return new CollectdTypes(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public List<WriteMetric> convert(final CollectdSample sample,
            final Iterable<Map.Entry<String, String>> tags) {
        final Mapping mapping = mappings.get(sample.getPlugin());

        if (mapping == null) {
            return convertDefault(sample, tags);
        }

        return mapping.convert(sample, tags);
    }

    /**
     * Default conversion of collectd samples.
     *
     * This
     */
    private List<WriteMetric> convertDefault(final CollectdSample sample,
            final Iterable<Map.Entry<String, String>> tags) {
        final long time = sample.getTime() * 1000;

        final Iterator<CollectdValue> values = sample.getValues().iterator();
        final ImmutableList.Builder<WriteMetric> writes = ImmutableList.builder();
        final Iterable<Map.Entry<String, String>> sampleTags = defaultTags(sample);

        while (values.hasNext()) {
            final CollectdValue value = values.next();

            final Series series = Series.of(key, Iterables.concat(tags, sampleTags).iterator());
            final Point point = new Point(time, value.toDouble());

            final MetricCollection data = MetricCollection.points(ImmutableList.of(point));

            writes.add(new WriteMetric(series, data));
        }

        return writes.build();
    }

    private Iterable<Map.Entry<String, String>> defaultTags(final CollectdSample sample) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        if (!"".equals(sample.getPlugin())) {
            builder.put(pluginTag, sample.getPlugin());
        }

        if (!"".equals(sample.getPluginInstance())) {
            builder.put(pluginInstanceTag, sample.getPluginInstance());
        }

        if (!"".equals(sample.getType())) {
            builder.put(typeTag, sample.getType());
        }

        if (!"".equals(sample.getTypeInstance())) {
            builder.put(typeInstanceTag, sample.getTypeInstance());
        }

        return builder.build().entrySet();
    }

    @Data
    private class Mapping {
        private final List<Field> fields;

        public List<WriteMetric> convert(final CollectdSample sample,
                final Iterable<Map.Entry<String, String>> tags) {
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

                final Series series = Series.of(key,
                        Iterables.concat(tags, field.tags(sample, value).entrySet()).iterator());
                final Point point = new Point(time, value.convert(field));

                final MetricCollection data = MetricCollection.points(ImmutableList.of(point));

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

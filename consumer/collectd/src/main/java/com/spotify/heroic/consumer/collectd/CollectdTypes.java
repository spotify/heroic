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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.WriteOptions;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class CollectdTypes {
    private static final Map<String, Mapping> MAPPINGS = new HashMap<>();

    public static final Plugin DISK = s -> ImmutableMap.of("disk", s.getPluginInstance());
    public static final Plugin DF_COMPLEX =
        s -> ImmutableMap.of("what", String.format("df-complex-%s", s.getTypeInstance()), "disk",
            s.getPluginInstance());

    public static final Plugin INTERFACE = s -> ImmutableMap.of("interface", s.getPluginInstance());

    public static final Plugin LOAD = s -> ImmutableMap.of("interface", s.getPluginInstance());

    static {
        type("disk_latency", mapping(DISK, what("disk-latency-read"), what("disk-latency-write")));
        type("disk_merged", mapping(DISK, what("disk-merged-read"), what("disk-merged-write")));
        type("disk_ops", mapping(DISK, what("disk-ops-read"), what("disk-ops-write")));
        type("disk_octets", mapping(DISK, what("disk-octets-read"), what("disk-octets-write")));
        type("disk_time", mapping(DISK, what("disk-time-read"), what("disk-time-write")));
        type("disk_io_time", mapping(DISK, what("disk-io-time-read"), what("disk-io-time-write")));
        type("pending_operations", mapping(DISK, what("disk-pending-operations")));

        type("df_complex", mapping(DF_COMPLEX, empty()));

        type("if_octets", mapping(INTERFACE, what("if-octets-rx"), what("if-octets-tx")));
        type("if_packets", mapping(INTERFACE, what("if-packets-rx"), what("if-packets-tx")));
        type("if_errors", mapping(INTERFACE, what("if-errors-rx"), what("if-errors-tx")));

        type("load",
            mapping(LOAD, what("load-shortterm"), what("load-midterm"), what("load-longterm")));
    }

    private static void type(final String type, final Mapping mapping) {
        MAPPINGS.put(type, mapping);
    }

    public static final String DEFAULT_KEY = "collectd";

    private static final String DEFAULT_PLUGIN_TAG = "plugin";
    private static final String DEFAULT_PLUGIN_INSTANCE_TAG = "plugin_instance";
    private static final String DEFAULT_TYPE_TAG = "type";
    private static final String DEFAULT_TYPE_INSTANCE_TAG = "type_instance";

    private final Map<String, Mapper> mappings;
    private final String key;
    private final String pluginTag;
    private final String pluginInstanceTag;
    private final String typeTag;
    private final String typeInstanceTag;

    @JsonCreator
    public CollectdTypes(
        @JsonProperty("key") Optional<String> key,
        @JsonProperty("pluginTag") Optional<String> pluginTag,
        @JsonProperty("pluginInstanceTag") Optional<String> pluginInstanceTag,
        @JsonProperty("typeTag") Optional<String> typeTag,
        @JsonProperty("typeInstanceTag") Optional<String> typeInstanceTag
    ) {
        this.key = key.orElse(DEFAULT_KEY);
        this.pluginTag = pluginTag.orElse(DEFAULT_PLUGIN_TAG);
        this.pluginInstanceTag = pluginInstanceTag.orElse(DEFAULT_PLUGIN_INSTANCE_TAG);
        this.typeTag = typeTag.orElse(DEFAULT_TYPE_TAG);
        this.typeInstanceTag = typeInstanceTag.orElse(DEFAULT_TYPE_INSTANCE_TAG);
        this.mappings = setupMappings();
    }

    private Map<String, Mapper> setupMappings() {
        final ImmutableMap.Builder<String, Mapper> builder = ImmutableMap.builder();

        for (final Map.Entry<String, Mapping> m : MAPPINGS.entrySet()) {
            builder.put(m.getKey(), m.getValue().setup(this));
        }

        return builder.build();
    }

    public static CollectdTypes supplyDefault() {
        return new CollectdTypes(Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());
    }

    public List<Ingestion.Request> convert(
        final CollectdSample sample, final Iterable<Map.Entry<String, String>> tags
    ) {
        final Mapper mapping = mappings.get(sample.getType());

        if (mapping == null) {
            log.info("No mapping found for sample {} {}", sample, tags);
            return convertDefault(sample, tags);
        }

        return mapping.convert(sample, tags);
    }

    /**
     * Default conversion of collectd samples.
     */
    private List<Ingestion.Request> convertDefault(
        final CollectdSample sample, final Iterable<Map.Entry<String, String>> tags
    ) {
        final long time = sample.getTime() * 1000;

        final Iterator<CollectdValue> values = sample.getValues().iterator();
        final ImmutableList.Builder<Ingestion.Request> ingestions = ImmutableList.builder();
        final Iterable<Map.Entry<String, String>> sampleTags = defaultTags(sample);

        while (values.hasNext()) {
            final CollectdValue value = values.next();

            final Series series = Series.of(key, Iterables.concat(tags, sampleTags).iterator());
            final Point point = new Point(time, value.toDouble());

            final MetricCollection data = MetricCollection.points(ImmutableList.of(point));

            final Ingestion.Request request = new Ingestion.Request(WriteOptions.defaults(),
                series, data);

            ingestions.add(request);
        }

        return ingestions.build();
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

    public static Mapping mapping(final Plugin plugin, final Field... fields) {
        return new Mapping(plugin, fields);
    }

    public static Field empty() {
        return new Field(0, Double.POSITIVE_INFINITY);
    }

    public static Field what(final String what) {
        return new WhatField(what, 0, Double.POSITIVE_INFINITY);
    }

    public static Field what(final String what, final double low, final double high) {
        return new WhatField(what, low, high);
    }

    interface Plugin {
        Map<String, String> tags(CollectdSample sample);
    }

    interface Mapper {
        List<Ingestion.Request> convert(
            final CollectdSample sample, final Iterable<Map.Entry<String, String>> tags
        );
    }

    static class Mapping {
        private final Plugin plugin;
        private final List<Field> fields;

        public Mapping(final Plugin plugin, final Field... fields) {
            this.plugin = plugin;
            this.fields = ImmutableList.copyOf(fields);
        }

        public Mapper setup(CollectdTypes types) {
            return (sample, tags) -> {
                final long time = sample.getTime() * 1000;

                final Iterator<Field> fields = this.fields.iterator();
                final Iterator<CollectdValue> values = sample.getValues().iterator();

                final ImmutableList.Builder<Ingestion.Request> ingestions = ImmutableList.builder();

                final Map<String, String> base = plugin.tags(sample);

                while (fields.hasNext()) {
                    if (!values.hasNext()) {
                        throw new IllegalArgumentException("too few values for mapping");
                    }

                    final Field field = fields.next();
                    final CollectdValue value = values.next();

                    final Series series = Series.of(types.key, Iterables
                        .concat(tags, base.entrySet(), field.tags(sample, value).entrySet())
                        .iterator());
                    final Point point = new Point(time, value.convert(field));

                    final MetricCollection data = MetricCollection.points(ImmutableList.of(point));

                    final Ingestion.Request request =
                        new Ingestion.Request(WriteOptions.defaults(), series, data);

                    ingestions.add(request);
                }

                return ingestions.build();
            };
        }
    }

    @RequiredArgsConstructor
    static class Field {
        private final double lower;
        private final double upper;

        public double convertCounter(long counter) {
            return Long.valueOf(counter).doubleValue();
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

        public Map<String, String> tags(final CollectdSample s, final CollectdValue v) {
            return ImmutableMap.of();
        }
    }

    static class WhatField extends Field {
        private final String what;

        public WhatField(final String what, final double lower, final double upper) {
            super(lower, upper);
            this.what = what;
        }

        @Override
        public Map<String, String> tags(final CollectdSample s, final CollectdValue v) {
            final ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();
            tags.put("what", what);
            return tags.build();
        }
    }
}

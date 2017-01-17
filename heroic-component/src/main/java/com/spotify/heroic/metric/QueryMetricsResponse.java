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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;

import lombok.Data;
import lombok.NonNull;

@Data
@JsonSerialize(using = QueryMetricsResponse.Serializer.class)
public class QueryMetricsResponse {
    @NonNull
    private final UUID queryId;

    @NonNull
    private final DateRange range;

    @NonNull
    private final List<ShardedResultGroup> result;

    @NonNull
    private final Statistics statistics = Statistics.empty();

    @NonNull
    private final List<RequestError> errors;

    @NonNull
    private final QueryTrace trace;

    @NonNull
    private final ResultLimits limits;

    public static class Serializer extends JsonSerializer<QueryMetricsResponse> {
        @Override
        public void serialize(
            QueryMetricsResponse response, JsonGenerator g, SerializerProvider provider
        ) throws IOException {
            final List<ShardedResultGroup> result = response.getResult();
            final Map<String, SortedSet<String>> common = calculateCommon(g, result);

            g.writeStartObject();

            g.writeObjectField("queryId", response.getQueryId());
            g.writeObjectField("range", response.getRange());
            g.writeObjectField("trace", response.getTrace());
            g.writeObjectField("limits", response.getLimits());

            g.writeFieldName("commonTags");
            serializeCommonTags(g, common);

            g.writeFieldName("result");
            serializeResult(g, common, result);

            g.writeFieldName("errors");
            serializeErrors(g, response.getErrors());

            g.writeEndObject();
        }

        private void serializeCommonTags(
            final JsonGenerator g, final Map<String, SortedSet<String>> common
        ) throws IOException {
            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> e : common.entrySet()) {
                g.writeFieldName(e.getKey());

                g.writeStartArray();

                for (final String value : e.getValue()) {
                    g.writeString(value);
                }

                g.writeEndArray();
            }

            g.writeEndObject();
        }

        private void serializeErrors(final JsonGenerator g, final List<RequestError> errors)
            throws IOException {
            g.writeStartArray();

            for (final RequestError error : errors) {
                g.writeObject(error);
            }

            g.writeEndArray();
        }

        private Map<String, SortedSet<String>> calculateCommon(
            final JsonGenerator g, final List<ShardedResultGroup> result
        ) {
            final Map<String, SortedSet<String>> common = new HashMap<>();
            final Set<String> blacklist = new HashSet<>();

            for (final ShardedResultGroup r : result) {
                final Set<Map.Entry<String, SortedSet<String>>> entries =
                    SeriesValues.fromSeries(r.getSeries().iterator()).getTags().entrySet();

                for (final Map.Entry<String, SortedSet<String>> e : entries) {
                    if (blacklist.contains(e.getKey())) {
                        continue;
                    }

                    final SortedSet<String> previous = common.put(e.getKey(), e.getValue());

                    if (previous == null) {
                        continue;
                    }

                    if (previous.equals(e.getValue())) {
                        continue;
                    }

                    blacklist.add(e.getKey());
                    common.remove(e.getKey());
                }
            }

            return common;
        }

        private void serializeResult(
            final JsonGenerator g, final Map<String, SortedSet<String>> common,
            final List<ShardedResultGroup> result
        ) throws IOException {

            g.writeStartArray();

            for (final ShardedResultGroup group : result) {
                g.writeStartObject();

                final MetricCollection collection = group.getMetrics();
                final SeriesValues series = SeriesValues.fromSeries(group.getSeries().iterator());

                g.writeStringField("type", collection.getType().identifier());
                g.writeStringField("hash", Integer.toHexString(group.hashGroup()));
                g.writeObjectField("shard", group.getShard());
                g.writeNumberField("cadence", group.getCadence());
                g.writeObjectField("values", collection.getData());

                writeKey(g, series.getKeys());
                writeTags(g, common, series.getTags());
                writeTagCounts(g, series.getTags());

                g.writeEndObject();
            }

            g.writeEndArray();
        }

        void writeKey(JsonGenerator g, final SortedSet<String> keys) throws IOException {
            g.writeFieldName("key");

            if (keys.size() == 1) {
                g.writeString(keys.iterator().next());
            } else {
                g.writeNull();
            }
        }

        void writeTags(
            JsonGenerator g, final Map<String, SortedSet<String>> common,
            final Map<String, SortedSet<String>> tags
        ) throws IOException {
            g.writeFieldName("tags");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                // TODO: enable this when commonTags is used.
                /*if (common.containsKey(pair.getKey())) {
                    continue;
                }*/

                final SortedSet<String> values = pair.getValue();

                if (values.size() != 1) {
                    continue;
                }

                g.writeStringField(pair.getKey(), values.iterator().next());
            }

            g.writeEndObject();
        }

        void writeTagCounts(JsonGenerator g, final Map<String, SortedSet<String>> tags)
            throws IOException {
            g.writeFieldName("tagCounts");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() <= 1) {
                    continue;
                }

                g.writeNumberField(pair.getKey(), values.size());
            }

            g.writeEndObject();
        }
    }

    public Summary summarize() {
        return new Summary(range, ShardedResultGroup.summarize(result), statistics, errors, trace,
            limits);
    }

    // Only include data suitable to log to query log
    @Data
    public class Summary {
        private final DateRange range;
        private final ShardedResultGroup.MultiSummary result;
        private final Statistics statistics;
        private final List<RequestError> errors;
        private final QueryTrace trace;
        private final ResultLimits limits;
    }
}

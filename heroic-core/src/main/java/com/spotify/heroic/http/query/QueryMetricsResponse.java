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

package com.spotify.heroic.http.query;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardLatency;
import com.spotify.heroic.metric.ShardTrace;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.TagValues;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    private static final String SERIES = "series";
    private static final String EVENTS = "events";

    @Getter
    private final DateRange range;

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final List<ShardedResultGroup> result;

    @Getter
    private final Statistics statistics;

    @Getter
    private final List<RequestError> errors;

    /**
     * Shard latencies associated with the query.
     * @deprecated Use {@link #trace} instead.
     */
    @Getter
    private final List<ShardLatency> latencies = ImmutableList.of();

    @Getter
    private final ShardTrace trace;

    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g, SerializerProvider provider)
                throws IOException, JsonProcessingException {

            final List<ShardedResultGroup> result = (List<ShardedResultGroup>) value;

            g.writeStartArray();

            for (final ShardedResultGroup resultGroup : result) {
                final List<TagValues> tags = resultGroup.getTags();
                final MetricTypedGroup group = resultGroup.getGroup();

                switch (group.getType()) {
                case POINT:
                    writeDataPoints(g, resultGroup, tags, group.getDataAs(Point.class));
                    break;
                case EVENT:
                    writeEvents(g, resultGroup, tags, group.getDataAs(Event.class));
                    break;
                default:
                    /* ignore, for now */
                    break;
                }
            }

            g.writeEndArray();
        }

        private void writeDataPoints(JsonGenerator g, final ShardedResultGroup group, final List<TagValues> tags,
                final List<Point> datapoints) throws IOException {
            g.writeStartObject();
            g.writeFieldName("type");

            g.writeStringField("type", SERIES);
            g.writeStringField("hash", Integer.toHexString(group.hashCode()));
            g.writeObjectField("shard", group.getShard());
            g.writeNumberField("cadence", group.getCadence());

            g.writeFieldName("tags");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                final List<String> values = pair.getValues();

                if (values.size() != 1)
                    continue;

                g.writeFieldName(pair.getKey());
                g.writeString(values.iterator().next());
            }

            g.writeEndObject();

            g.writeFieldName("tagCounts");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                final List<String> values = pair.getValues();

                if (values.size() <= 1)
                    continue;

                g.writeFieldName(pair.getKey());
                g.writeNumber(values.size());
            }

            g.writeEndObject();

            g.writeObjectField("values", datapoints);

            g.writeEndObject();
        }

        private void writeEvents(JsonGenerator g, final ShardedResultGroup group, final List<TagValues> tags,
                final List<Event> events) throws IOException {
            g.writeStartObject();

            g.writeStringField("type", EVENTS);
            g.writeStringField("hash", Integer.toHexString(group.hashCode()));
            g.writeObjectField("shard", group.getShard());
            g.writeNumberField("cadence", group.getCadence());

            g.writeFieldName("tags");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                g.writeFieldName(pair.getKey());
                g.writeObject(pair.getValues());
            }

            g.writeEndObject();

            g.writeObjectField("values", events);

            g.writeEndObject();
        }
    }
}

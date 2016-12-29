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

package com.spotify.heroic.consumer.schemas;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.consumer.SchemaScope;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.ingestion.WriteOptions;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.statistics.ConsumerReporter;
import dagger.Component;
import lombok.Data;
import lombok.ToString;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString
public class Spotify100 implements ConsumerSchema {
    private static final String HOST = "host";
    private static final String KEY = "key";
    private static final String TIME = "time";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String SCHEMA_VERSION = "1.0.0";

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JsonMetric {
        private final String version;
        private final String key;
        private final String host;
        private final Long time;
        @JsonDeserialize(using = TagsDeserializer.class)
        private final Map<String, String> attributes;
        private final Double value;

        @JsonCreator
        public JsonMetric(
            @JsonProperty("version") String version, @JsonProperty("key") String key,
            @JsonProperty("host") String host, @JsonProperty("time") Long time,
            @JsonProperty("attributes") Map<String, String> attributes,
            @JsonProperty("value") Double value
        ) {
            this.version = version;
            this.key = key;
            this.host = host;
            this.time = time;
            this.attributes = attributes;
            this.value = value;
        }

        public static final class TagsDeserializer extends JsonDeserializer<Map<String, String>> {
            @Override
            public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
                final ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();

                if (p.getCurrentToken() != JsonToken.START_OBJECT) {
                    throw ctxt.wrongTokenException(p, JsonToken.START_OBJECT, null);
                }

                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    final String key = p.getCurrentName();
                    final String value = p.nextTextValue();

                    if (value == null) {
                        continue;
                    }

                    tags.put(key, value);
                }

                if (p.getCurrentToken() != JsonToken.END_OBJECT) {
                    throw ctxt.wrongTokenException(p, JsonToken.END_OBJECT, null);
                }

                return tags.build();
            }
        }
    }

    @SchemaScope
    public static class Consumer implements ConsumerSchema.Consumer {
        private final IngestionGroup ingestion;
        private final ConsumerReporter reporter;

        @Inject
        public Consumer(IngestionGroup ingestion, ConsumerReporter reporter) {
            this.ingestion = ingestion;
            this.reporter = reporter;
        }

        @Override
        public void consume(final byte[] message) throws ConsumerSchemaException {
            final JsonMetric metric;

            try {
                metric = mapper.readValue(message, JsonMetric.class);
            } catch (final Exception e) {
                throw new ConsumerSchemaValidationException("Received invalid metric", e);
            }

            if (metric.getValue() == null) {
                throw new ConsumerSchemaValidationException(
                    "Metric must have a value but this metric has a null value: " + metric);
            }

            if (metric.getVersion() == null || !SCHEMA_VERSION.equals(metric.getVersion())) {
                throw new ConsumerSchemaValidationException(
                    String.format("Invalid version %s, expected %s", metric.getVersion(),
                        SCHEMA_VERSION));
            }

            if (metric.getTime() == null) {
                throw new ConsumerSchemaValidationException(
                    "'" + TIME + "' field must be defined: " + message);
            }

            if (metric.getKey() == null) {
                throw new ConsumerSchemaValidationException(
                    "'" + KEY + "' field must be defined: " + message);
            }

            final Map<String, String> tags = new HashMap<String, String>(metric.getAttributes());
            tags.put(HOST, metric.getHost());

            final Series series = Series.of(metric.getKey(), tags);
            final Point p = new Point(metric.getTime(), metric.getValue());
            final List<Point> points = ImmutableList.of(p);

            final Ingestion.Request request =
                new Ingestion.Request(WriteOptions.defaults(), series,
                    MetricCollection.points(points));

            reporter.reportMessageDrift(System.currentTimeMillis() - p.getTimestamp());
            ingestion.write(request);
        }
    }

    @Override
    public Exposed setup(final ConsumerSchema.Depends depends) {
        return DaggerSpotify100_C.builder().depends(depends).build();
    }

    @SchemaScope
    @Component(dependencies = ConsumerSchema.Depends.class)
    interface C extends ConsumerSchema.Exposed {
        @Override
        Consumer consumer();
    }
}

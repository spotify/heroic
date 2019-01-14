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

import static io.opencensus.trace.AttributeValue.stringAttributeValue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.consumer.SchemaScope;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import lombok.Data;
import lombok.ToString;

@ToString
public class Spotify100 implements ConsumerSchema {
    private static final String HOST_TAG = "host";
    private static final ObjectMapper mapper = objectMapper();
    private static final Tracer tracer = Tracing.getTracer();

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JsonMetric {
        private final String key;
        @Deprecated
        private final String host;
        private final Long time;
        @JsonDeserialize(using = TagsDeserializer.class)
        private final Map<String, String> attributes;
        @JsonDeserialize(using = TagsDeserializer.class)
        private final Map<String, String> resource;
        private final Double value;

        @JsonCreator
        public JsonMetric(
            @JsonProperty("key") String key,
            @JsonProperty("host") Optional<String> host,
            @JsonProperty("time") Long time,
            @JsonProperty("attributes") Map<String, String> attributes,
            @JsonProperty("resource") Map<String, String> resource,
            @JsonProperty("value") Double value
        ) {
            this.key = key;
            this.host = host.orElse(null);
            this.time = time;
            this.attributes = attributes;
            this.resource = resource;
            this.value = value;
        }

        public static final class TagsDeserializer extends JsonDeserializer<Map<String, String>> {
            @Override
            public Map<String, String> getNullValue(final DeserializationContext ctxt)
                throws JsonMappingException {
                return ImmutableMap.of();
            }

            @Override
            public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
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
        private final Clock clock;
        private final IngestionGroup ingestion;
        private final ConsumerReporter reporter;

        @Inject
        public Consumer(Clock clock, IngestionGroup ingestion, ConsumerReporter reporter) {
            this.clock = clock;
            this.ingestion = ingestion;
            this.reporter = reporter;
        }

        @Override
        public AsyncFuture<Void> consume(final byte[] message) throws ConsumerSchemaException {
            final JsonNode tree;
            final Span span = tracer.spanBuilder("ConsumerSchema.consume").startSpan();
            span.putAttribute("schema", stringAttributeValue("Spotify100"));

            try (Scope ws = tracer.withSpan(span)) {
                try {
                    tree = mapper.readTree(message);
                } catch (final Exception e) {
                    span.setStatus(Status.INVALID_ARGUMENT.withDescription(e.toString()));
                    span.end();
                    throw new ConsumerSchemaValidationException("Invalid metric", e);
                }

                if (tree.getNodeType() != JsonNodeType.OBJECT) {
                    span.setStatus(
                        Status.INVALID_ARGUMENT.withDescription("Metric is not an object"));
                    span.end();
                    throw new ConsumerSchemaValidationException(
                        "Expected object, but got: " + tree.getNodeType());
                }

                final ObjectNode object = (ObjectNode) tree;

                final JsonNode versionNode = object.remove("version");

                if (versionNode == null) {
                    span.setStatus(Status.INVALID_ARGUMENT.withDescription("Missing version"));
                    span.end();
                    throw new ConsumerSchemaValidationException(
                        "Missing version in received object");
                }

                final Version version;

                try {
                    version = Version.parse(versionNode.asText());
                } catch (final Exception e) {
                    span.setStatus(Status.INVALID_ARGUMENT.withDescription("Bad version"));
                    throw new ConsumerSchemaValidationException("Bad version: " + versionNode);
                }

                if (version.getMajor() == 1) {
                    return handleVersion1(tree).onFinished(span::end);
                }

                span.setStatus(Status.INVALID_ARGUMENT.withDescription("Unsupported version"));
                span.end();
                throw new ConsumerSchemaValidationException("Unsupported version: " + version);
            }
        }

        private AsyncFuture<Void> handleVersion1(final JsonNode tree)
            throws ConsumerSchemaValidationException {
            final JsonMetric metric;

            try {
                metric = new TreeTraversingParser(tree, mapper).readValueAs(JsonMetric.class);
            } catch (IOException e) {
                throw new ConsumerSchemaValidationException("Invalid metric", e);
            }

            if (metric.getValue() == null) {
                throw new ConsumerSchemaValidationException(
                    "Metric must have a value but this metric has a null value: " + metric);
            }

            if (metric.getTime() == null) {
                throw new ConsumerSchemaValidationException("time: field must be defined: " + tree);
            }

            if (metric.getTime() <= 0) {
                throw new ConsumerSchemaValidationException(
                    "time: field must be a positive number: " + tree);
            }

            if (metric.getKey() == null) {
                throw new ConsumerSchemaValidationException("key: field must be defined: " + tree);
            }

            final Map<String, String> tags = new HashMap<>(metric.getAttributes());

            if (metric.getHost() != null) {
                tags.put(HOST_TAG, metric.getHost());
            }

            final Map<String, String> resource = new HashMap<>(metric.getResource());

            final Series series = Series.of(metric.getKey(), tags, resource);
            final Point p = new Point(metric.getTime(), metric.getValue());
            final List<Point> points = ImmutableList.of(p);

            reporter.reportMessageDrift(clock.currentTimeMillis() - p.getTimestamp());
            AsyncFuture<Ingestion> ingestionFuture =
                ingestion.write(new Ingestion.Request(series, MetricCollection.points(points)));

            // Return Void future, to not leak unnecessary information from the backend but just
            // allow monitoring of when the consumption is done.
            return ingestionFuture.directTransform(future -> null);
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

    @Data
    static class Version {
        private final int major;
        private final int minor;
        private final int patch;

        /**
         * Parse the given version string.
         */
        public static Version parse(final String input) {
            if (input == null) {
                throw new NullPointerException();
            }

            final String[] parts = input.trim().split("\\.");

            if (parts.length != 3) {
                throw new IllegalArgumentException("too few components: " + input);
            }

            final int major = Integer.parseInt(parts[0]);
            final int minor = Integer.parseInt(parts[1]);
            final int patch = Integer.parseInt(parts[2]);

            return new Version(major, minor, patch);
        }
    }

    /**
     * Setup the ObjectMapper necessary to serialize types in this protocol.
     */
    static ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(true));
        return mapper;
    }
}

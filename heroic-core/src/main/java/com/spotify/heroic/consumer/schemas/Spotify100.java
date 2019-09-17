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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.consumer.SchemaScope;
import com.spotify.heroic.consumer.schemas.spotify100.JsonMetric;
import com.spotify.heroic.consumer.schemas.spotify100.Version;
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
import javax.inject.Inject;

public class Spotify100 implements ConsumerSchema {
    private static final String HOST_TAG = "host";
    private static final ObjectMapper mapper = objectMapper();
    private static final Tracer tracer = Tracing.getTracer();

    public String toString() {
        return "Spotify100()";
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
                    span.end();
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
            reporter.reportMetricsIn(1);
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

    /**
     * Setup the ObjectMapper necessary to serialize types in this protocol.
     */
    static ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(true))
            .registerModule(new KotlinModule());
    }
}

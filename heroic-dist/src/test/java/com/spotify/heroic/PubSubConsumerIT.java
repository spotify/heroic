/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assume.assumeNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.pubsub.EmulatorHelper;
import com.spotify.heroic.consumer.pubsub.PubSubConsumerModule;
import com.spotify.heroic.consumer.schemas.Spotify100;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.instrumentation.OperationsLogImpl;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.memory.MemoryMetricModule;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;

@NotThreadSafe
public class PubSubConsumerIT extends AbstractConsumerIT {
    private final String topic = "topic1";
    private final String subscription = "sub1";
    private final String project = "heroic-it-1";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private OperationsLogImpl opLog;
    private Publisher publisher;

    @Override
    protected HeroicConfig.Builder setupConfig() {
        assumeNotNull(EmulatorHelper.hostPort());

        opLog = new OperationsLogImpl();
        final MetricModule backingStore = MemoryMetricModule.builder().build();

        try {
            publisher = EmulatorHelper.publisher(project, topic);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        MetricModule metricModule = new LoggingMetricModule(backingStore, opLog);
        return HeroicConfig
            .builder()
            .stopTimeout(Duration.of(5, TimeUnit.SECONDS))
            .consumers(ImmutableList.of(PubSubConsumerModule
                .builder()
                .topicId(topic)
                .schema(Spotify100.class)
                .subscriptionId(subscription)
                .projectId(project)
            ))
            .ingestion(IngestionModule.builder().updateMetrics(true))
            .metrics(MetricManagerModule.builder().backends(ImmutableList.of(metricModule)));
    }

    @Override
    protected Consumer<WriteMetric.Request> setupConsumer() {
        return request -> {
            final MetricCollection mc = request.getData();

            if (mc.getType() != MetricType.POINT) {
                throw new RuntimeException("Unsupported metric type: " + mc.getType());
            }

            final Series series = request.getSeries();
            for (final Point p : mc.getDataAs(Point.class)) {
                final DataVersion1 src =
                    new DataVersion1("1.1.0", series.getKey(), "localhost", p.getTimestamp(),
                        series.getTags(), series.getResource(), p.getValue());

                final PubsubMessage pubsubMessage;

                try {
                    String message = objectMapper.writeValueAsString(src);
                    ByteString data = ByteString.copyFromUtf8(message);
                    pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                publisher.publish(pubsubMessage);
            }
        };
    }

    @After
    public void tearDownSubscription() throws IOException {
        EmulatorHelper.deleteSubscription(project, subscription);
    }

    @After
    public void verifyTransactionality() {
        long writeRequests = 0;
        long writeCompletions = 0;

        for (OperationsLogImpl.OpType op : opLog.getLog()) {
            if (op == OperationsLogImpl.OpType.WRITE_REQUEST) {
                writeRequests++;
            }

            if (op == OperationsLogImpl.OpType.WRITE_COMPLETE) {
                writeCompletions++;
            }
        }

        assertEquals(writeRequests, writeCompletions);
    }
}

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

package com.spotify.heroic.consumer.pubsub;

import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.shaded.pubsub.com.google.api.gax.batching.FlowControlSettings;
import com.spotify.shaded.pubsub.com.google.api.gax.core.CredentialsProvider;
import com.spotify.shaded.pubsub.com.google.api.gax.core.ExecutorProvider;
import com.spotify.shaded.pubsub.com.google.api.gax.core.InstantiatingExecutorProvider;
import com.spotify.shaded.pubsub.com.google.api.gax.core.NoCredentialsProvider;
import com.spotify.shaded.pubsub.com.google.api.gax.grpc.GrpcTransportChannel;
import com.spotify.shaded.pubsub.com.google.api.gax.rpc.AlreadyExistsException;
import com.spotify.shaded.pubsub.com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.spotify.shaded.pubsub.com.google.api.gax.rpc.TransportChannelProvider;
import com.spotify.shaded.pubsub.com.google.cloud.pubsub.v1.Subscriber;
import com.spotify.shaded.pubsub.com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.spotify.shaded.pubsub.com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.spotify.shaded.pubsub.com.google.cloud.pubsub.v1.TopicAdminClient;
import com.spotify.shaded.pubsub.com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.spotify.shaded.pubsub.com.google.pubsub.v1.ProjectSubscriptionName;
import com.spotify.shaded.pubsub.com.google.pubsub.v1.ProjectTopicName;
import com.spotify.shaded.pubsub.com.google.pubsub.v1.PushConfig;
import com.spotify.shaded.pubsub.io.grpc.ManagedChannel;
import com.spotify.shaded.pubsub.io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

@Slf4j
@Data
public class Connection {
    private final int threads;
    private final ConsumerSchema.Consumer consumer;
    private final long maxOutstandingElementCount;
    private final long maxOutstandingRequestBytes;
    private Subscriber subscriber = null;

    private final String projectId;
    private final String subscriptionId;
    private final ProjectTopicName topicName;
    private final ProjectSubscriptionName subscriptionName;

    private CredentialsProvider credentialsProvider;
    private TransportChannelProvider channelProvider;

    Connection(
        ConsumerSchema.Consumer consumer,
        String projectId,
        String topicId,
        String subscriptionId,
        int threads,
        long maxOutstandingElementCount,
        long maxOutstandingRequestBytes,
        int maxInboundMessageSize,
        long keepAlive
    ) {
        this.consumer = consumer;
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.threads = threads;
        this.topicName = ProjectTopicName.of(projectId, topicId);
        this.subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        this.credentialsProvider = SubscriptionAdminSettings
            .defaultCredentialsProviderBuilder()
            .build();
        this.channelProvider = SubscriptionAdminSettings
            .defaultGrpcTransportProviderBuilder()
            .setMaxInboundMessageSize(maxInboundMessageSize)
            .setKeepAliveTime(Duration.ofSeconds(keepAlive))
            .build();
        this.maxOutstandingElementCount = maxOutstandingElementCount;
        this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
    }

    Connection start() {
        log.info("Starting PubSub connection");

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName
            .of(projectId, subscriptionId);

        FlowControlSettings flowControlSettings =
            FlowControlSettings.newBuilder()
                .setMaxOutstandingElementCount(maxOutstandingElementCount)
                .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
                .build();

        ExecutorProvider executorProvider =
            InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(threads).build();

        log.info("Subscribing to {}", subscriptionName);
        Subscriber subscriber = Subscriber
            .newBuilder(subscriptionName, new Receiver(consumer))
            .setFlowControlSettings(flowControlSettings)
            .setExecutorProvider(executorProvider)
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();

        subscriber.startAsync().awaitRunning();
        log.info("PubSub connection started");
        return this;
    }

    public Connection shutdown() {
        log.info("Stopping PubSub connection");
        if (subscriber != null) {
            subscriber.stopAsync().awaitTerminated();
        }
        subscriber = null;
        return this;
    }

    /*
    Determine if a PubSub emulator should be used instead of a live connection. Sets the appropriate
    options on the builder when using the emulator.
     */
    void setEmulatorOptions() {
        String host = System.getenv("PUBSUB_EMULATOR_HOST");
        if (host == null) {
            return;
        }

        log.info("PubSub emulator detected at {}", host);
        ManagedChannel channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(
            GrpcTransportChannel.create(channel));
        credentialsProvider = NoCredentialsProvider.create();
    }

    /*
    Create the topic for metrics to be published to.
     */
    void createTopic() throws IOException {
        log.info("Creating topic {}", topicName);
        TopicAdminClient topicAdminClient = TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build()
        );
        try {
            topicAdminClient.createTopic(topicName);
        } catch (AlreadyExistsException e) {
            log.info("Topic already exists");
        }
    }

    /*
    Create a pull subscription if it does not exist.
     */
    void createSubscription() throws IOException {
        log.info("Creating subscription {} for topic {}", subscriptionName, topicName);
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
              .setTransportChannelProvider(channelProvider)
              .setCredentialsProvider(credentialsProvider)
              .build()
        );
        try {
            subscriptionAdminClient.createSubscription(
                subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        } catch (AlreadyExistsException e) {
            log.info("Subscription already exists");
        }
    }
}


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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;

public class EmulatorHelper {
    private static CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    private static TransportChannelProvider channelProvider(String endpoint) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }

    public static Publisher publisher(
        String projectId, String topicId, String endpoint
    ) throws IOException {
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);

        return Publisher.newBuilder(topicName)
            .setChannelProvider(channelProvider(endpoint))
            .setCredentialsProvider(credentialsProvider)
            .build();
    }

    public static void deleteSubscription(
        String projectId, String subscriptionId, String endpoint
    ) throws IOException {
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider(endpoint))
                .setCredentialsProvider(credentialsProvider)
                .build()
        );
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
        subscriptionAdminClient.deleteSubscription(subscriptionName);
    }
}

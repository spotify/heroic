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

package com.spotify.heroic.metric.datastax;

import java.util.Optional;
import java.util.Map;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Optional.empty;


public class DatastaxPoolingOptions {

    private final Optional<Map<HostDistance, Integer>> maxRequestsPerConnection;
    private final Optional<Map<HostDistance, Integer>> coreConnectionsPerHost;
    private final Optional<Map<HostDistance, Integer>> maxConnectionsPerHost;
    private final Optional<Map<HostDistance, Integer>> newConnectionThreshold;
    private final Optional<Integer> maxQueueSize;
    private final Optional<Integer> poolTimeoutMillis;
    private final Optional<Integer> idleTimeoutSeconds;
    private final Optional<Integer> heartbeatIntervalSeconds;

    public DatastaxPoolingOptions() {
        this(empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty());
    }

    @JsonCreator
    public DatastaxPoolingOptions(
        @JsonProperty("maxRequestsPerConnection")
            Optional<Map<HostDistance, Integer>> maxRequestsPerConnection,
        @JsonProperty("coreConnectionsPerHost")
            Optional<Map<HostDistance, Integer>> coreConnectionsPerHost,
        @JsonProperty("maxConnectionsPerHost")
            Optional<Map<HostDistance, Integer>> maxConnectionsPerHost,
        @JsonProperty("newConnectionThreshold")
            Optional<Map<HostDistance, Integer>> newConnectionThreshold,
        @JsonProperty("maxQueueSize") Optional<Integer> maxQueueSize,
        @JsonProperty("poolTimeoutMillis") Optional<Integer> poolTimeoutMillis,
        @JsonProperty("idleTimeoutSeconds") Optional<Integer> idleTimeoutSeconds,
        @JsonProperty("heartbeatIntervalSeconds") Optional<Integer> heartbeatIntervalSeconds
    ) {
        this.maxRequestsPerConnection = maxRequestsPerConnection;
        this.coreConnectionsPerHost = coreConnectionsPerHost;
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        this.newConnectionThreshold = newConnectionThreshold;
        this.maxQueueSize = maxQueueSize;
        this.poolTimeoutMillis = poolTimeoutMillis;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    public void apply(final Builder builder) {
        final PoolingOptions pooling = new PoolingOptions();
        this.maxRequestsPerConnection.ifPresent(x -> {
            for (Map.Entry<HostDistance, Integer> entry : x.entrySet()) {
                pooling.setMaxRequestsPerConnection(entry.getKey(), entry.getValue());
            }
        });
        this.coreConnectionsPerHost.ifPresent(x -> {
            for (Map.Entry<HostDistance, Integer> entry : x.entrySet()) {
                pooling.setCoreConnectionsPerHost(entry.getKey(), entry.getValue());
            }
        });
        this.maxConnectionsPerHost.ifPresent(x -> {
            for (Map.Entry<HostDistance, Integer> entry : x.entrySet()) {
                pooling.setMaxConnectionsPerHost(entry.getKey(), entry.getValue());
            }
        });
        this.newConnectionThreshold.ifPresent(x -> {
            for (Map.Entry<HostDistance, Integer> entry : x.entrySet()) {
                pooling.setNewConnectionThreshold(entry.getKey(), entry.getValue());
            }
        });

        this.maxQueueSize.ifPresent(pooling::setMaxQueueSize);
        this.poolTimeoutMillis.ifPresent(pooling::setPoolTimeoutMillis);
        this.idleTimeoutSeconds.ifPresent(pooling::setIdleTimeoutSeconds);
        this.heartbeatIntervalSeconds.ifPresent(pooling::setHeartbeatIntervalSeconds);
        builder.withPoolingOptions(pooling);
    }


}

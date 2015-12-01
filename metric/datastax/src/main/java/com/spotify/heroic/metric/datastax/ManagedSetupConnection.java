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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.metric.datastax.schema.Schema;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ManagedSetup;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@ToString(of = {"seeds"})
@Slf4j
public class ManagedSetupConnection implements ManagedSetup<Connection> {
    private final AsyncFramework async;
    private final Collection<InetSocketAddress> seeds;
    private final Schema schema;
    private final boolean configure;
    private final int fetchSize;
    private final Duration readTimeout;
    private final ConsistencyLevel consistencyLevel;
    private final RetryPolicy retryPolicy;

    public AsyncFuture<Connection> construct() {
        AsyncFuture<Session> session = async.call(new Callable<Session>() {
            public Session call() throws Exception {
                // @formatter:off
                final PoolingOptions pooling = new PoolingOptions();

                final QueryOptions queryOptions = new QueryOptions()
                    .setFetchSize(fetchSize)
                    .setConsistencyLevel(consistencyLevel);

                final SocketOptions socketOptions = new SocketOptions()
                    .setReadTimeoutMillis((int) readTimeout.toMilliseconds());

                final Cluster cluster = Cluster.builder()
                    .addContactPointsWithPorts(seeds)
                    .withRetryPolicy(retryPolicy)
                    .withPoolingOptions(pooling)
                    .withQueryOptions(queryOptions)
                    .withSocketOptions(socketOptions)
                    .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                    .build();
                // @formatter:on

                return cluster.connect();
            }
        });

        if (configure) {
            session = session.lazyTransform(s -> {
                return schema.configure(s).directTransform(i -> s);
            });
        }

        return session.lazyTransform(s -> {
            return schema.instance(s).directTransform(schema -> {
                return new Connection(s, schema);
            });
        });
    }

    @Override
    public AsyncFuture<Void> destruct(final Connection c) {
        return Async.bind(async, c.session.closeAsync()).directTransform(ign -> null);
    }
}

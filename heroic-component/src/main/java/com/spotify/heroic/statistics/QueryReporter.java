/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.statistics;

public interface QueryReporter {
    /**
     * Report on a full query, on an API node level
     */
    FutureReporter.Context reportQuery();

    /**
     * Report latency of a full query, including gathering data from shards and doing aggregation,
     * but only for queries that are deemed "small".
     * The threshold for what is deemed "small" may be configurable and may change.
     *
     * @param duration Duration of query, in ms
     */
    void reportSmallQueryLatency(long duration);

    void reportClusterNodeRpcError();
    void reportClusterNodeRpcCancellation();
}

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

package com.spotify.heroic;

import java.util.Collection;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.metric.QueryResult;

import eu.toolchain.async.AsyncFuture;

public interface QueryManager {
    Group useGroup(String group);

    Collection<? extends Group> useGroupPerNode(String group);

    Group useDefaultGroup();

    Collection<? extends Group> useDefaultGroupPerNode();

    QueryBuilder newQuery();

    QueryBuilder newQueryFromString(String query);

    String queryToString(final Query query);

    AsyncFuture<Void> initialized();

    public interface Group extends Iterable<ClusterNode.Group> {
        AsyncFuture<QueryResult> query(Query query);

        ClusterNode.Group first();
    }
}

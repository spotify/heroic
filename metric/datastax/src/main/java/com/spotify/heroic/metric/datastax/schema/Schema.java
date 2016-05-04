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

package com.spotify.heroic.metric.datastax.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchema;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchema;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LegacySchema.class, name = "legacy"), @JsonSubTypes.Type(
    value = NextGenSchema.class, name = "ng")
})
public interface Schema {
    public AsyncFuture<Void> configure(final Session session);

    public AsyncFuture<SchemaInstance> instance(final Session session);

    public static interface PreparedFetch {
        public BoundStatement fetch(int limit);

        public Transform<Row, Point> converter();
    }
}

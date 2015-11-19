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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;

import eu.toolchain.async.Transform;

public interface SchemaInstance {
    public TypeSerializer<MetricsRowKey> rowKey();

    public List<PreparedFetch> ranges(final Series series, final DateRange range)
            throws IOException;

    public PreparedFetch row(final BackendKey key) throws IOException;

    public BoundStatement deleteKey(ByteBuffer k);

    public BoundStatement countKey(ByteBuffer k);

    public WriteSession writeSession();

    public Transform<Row, BackendKey> keyConverter();

    public BackendKeyUtils keyUtils();

    public static interface WriteSession {
        public BoundStatement writePoint(Series series, Point d) throws IOException;
    }
}

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

import com.datastax.driver.core.Row;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import eu.toolchain.async.Transform;
import java.util.Optional;

public abstract class AbstractSchemaInstance implements SchemaInstance {
    protected final String key;

    @java.beans.ConstructorProperties({ "key" })
    public AbstractSchemaInstance(final String key) {
        this.key = key;
    }

    @Override
    public Transform<Row, BackendKey> keyConverter() {
        return row -> {
            final MetricsRowKey key = rowKey().deserialize(row.getBytes(this.key));
            return new BackendKey(key.getSeries(), key.getBase(), MetricType.POINT,
                Optional.of(row.getLong(1)));
        };
    }
}

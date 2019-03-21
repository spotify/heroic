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

import com.datastax.driver.core.Session;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;

public final class Connection {
    protected final Session session;
    protected final SchemaInstance schema;

    @java.beans.ConstructorProperties({ "session", "schema" })
    public Connection(final Session session, final SchemaInstance schema) {
        this.session = session;
        this.schema = schema;
    }
}

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

package com.spotify.heroic.metric.datastax.schema.ng;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Exposed;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;

import eu.toolchain.async.AsyncFramework;

public class NextGenSchemaModule implements SchemaModule {
    public static final String DEFAULT_KEYSPACE = "heroic";

    private final String keyspace;

    @JsonCreator
    public NextGenSchemaModule(@JsonProperty("keyspace") String keyspace) {
        this.keyspace = Optional.ofNullable(keyspace).orElse(DEFAULT_KEYSPACE);
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            @Exposed
            public Schema schema(final AsyncFramework async) {
                return new NextGenSchema(async, keyspace);
            }

            @Override
            protected void configure() {
            };
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String keyspace;

        public Builder keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public SchemaModule build() {
            return new NextGenSchemaModule(keyspace);
        }
    }
}

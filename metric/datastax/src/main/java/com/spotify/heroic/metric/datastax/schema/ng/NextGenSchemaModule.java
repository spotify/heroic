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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.metric.datastax.schema.SchemaComponent;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.SchemaScope;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import java.util.Optional;

public class NextGenSchemaModule implements SchemaModule {
    public static final String DEFAULT_KEYSPACE = "heroic";

    private final String keyspace;

    @JsonCreator
    public NextGenSchemaModule(@JsonProperty("keyspace") String keyspace) {
        this.keyspace = Optional.ofNullable(keyspace).orElse(DEFAULT_KEYSPACE);
    }

    @Override
    public SchemaComponent module(PrimaryComponent primary) {
        return DaggerNextGenSchemaModule_C.builder().primaryComponent(primary).m(new M()).build();
    }

    @SchemaScope
    @Component(modules = M.class, dependencies = PrimaryComponent.class)
    interface C extends SchemaComponent {
        @Override
        NextGenSchema schema();
    }

    @Module
    class M {
        @Provides
        @SchemaScope
        @Named("keyspace")
        public String keyspace() {
            return keyspace;
        }
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

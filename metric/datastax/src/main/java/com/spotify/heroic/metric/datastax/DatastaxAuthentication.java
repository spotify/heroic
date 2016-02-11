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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Optional;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(DatastaxAuthentication.None.class), @JsonSubTypes.Type(
    DatastaxAuthentication.Plain.class)
})
public interface DatastaxAuthentication {
    void accept(final Cluster.Builder builder);

    /**
     * No authentication null object.
     *
     * @author udoprog
     */
    @JsonTypeName("none")
    class None implements DatastaxAuthentication {
        @JsonCreator
        public None() {
        }

        @Override
        public void accept(Builder builder) {
        }
    }

    @JsonTypeName("plain")
    class Plain implements DatastaxAuthentication {
        public static final String DEFAULT_USERNAME = "cassandra";

        private final String username;
        private final String password;

        @JsonCreator
        public Plain(
            @JsonProperty("username") Optional<String> username,
            @JsonProperty("password") Optional<String> password
        ) {
            this.username = username.orElse(DEFAULT_USERNAME);
            this.password =
                password.orElseThrow(() -> new IllegalArgumentException("password is required"));
        }

        @Override
        public void accept(final Builder builder) {
            builder.withAuthProvider(new PlainTextAuthProvider(username, password));
        }
    }
}

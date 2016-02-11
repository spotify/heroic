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

package com.spotify.heroic.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.eclipse.jetty.server.HttpConfiguration;

import java.util.Optional;

@RequiredArgsConstructor
public class JettyHttpConfiguration {
    public static final boolean DEFAULT_SEND_SERVER_VERSION = false;

    private final boolean sendServerVersion;

    public HttpConfiguration build() {
        final HttpConfiguration c = new HttpConfiguration();
        c.setSendServerVersion(sendServerVersion);
        return c;
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder {
        private Optional<Boolean> sendServerVersion = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("sendServerVersion") Optional<Boolean> sendServerVersion) {
            this.sendServerVersion = sendServerVersion;
        }

        public JettyHttpConfiguration build() {
            return new JettyHttpConfiguration(
                sendServerVersion.orElse(DEFAULT_SEND_SERVER_VERSION));
        }
    }
}

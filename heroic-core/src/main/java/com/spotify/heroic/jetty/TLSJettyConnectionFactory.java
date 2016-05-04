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
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Optional;

@RequiredArgsConstructor
public class TLSJettyConnectionFactory implements JettyConnectionFactory {
    private final Optional<String> keyStorePath;
    private final Optional<String> keyStorePassword;
    private final Optional<String> keyManagerPassword;
    private final Optional<Boolean> trustAll;

    private final String nextProtocol;

    @Override
    public ConnectionFactory setup(final HttpConfiguration config) {
        final SslContextFactory context = new SslContextFactory();
        keyStorePath.ifPresent(context::setKeyStorePath);
        keyStorePassword.ifPresent(context::setKeyStorePassword);
        keyManagerPassword.ifPresent(context::setKeyManagerPassword);
        trustAll.ifPresent(context::setTrustAll);
        return new SslConnectionFactory(context, nextProtocol);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder implements JettyConnectionFactory.Builder {
        private Optional<String> keyStorePath = Optional.empty();
        private Optional<String> keyStorePassword = Optional.empty();
        private Optional<String> keyManagerPassword = Optional.empty();
        private Optional<Boolean> trustAll = Optional.empty();

        private Optional<String> nextProtocol = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("nextProtocol") final Optional<String> nextProtocol) {
            this.nextProtocol = nextProtocol;
        }

        public Builder keyStorePath(final String keyStorePath) {
            this.keyStorePath = Optional.of(keyStorePath);
            return this;
        }

        public Builder keyStorePassword(final String keyStorePassword) {
            this.keyStorePassword = Optional.of(keyStorePassword);
            return this;
        }

        public Builder keyManagerPassword(final String keyManagerPassword) {
            this.keyManagerPassword = Optional.of(keyManagerPassword);
            return this;
        }

        public Builder trustAll(final boolean trustAll) {
            this.trustAll = Optional.of(trustAll);
            return this;
        }

        public Builder nextProtocol(final String nextProtocol) {
            this.nextProtocol = Optional.of(nextProtocol);
            return this;
        }

        @Override
        public JettyConnectionFactory build() {
            final String nextProtocol = this.nextProtocol.orElse(HttpVersion.HTTP_1_1.toString());
            return new TLSJettyConnectionFactory(keyStorePath, keyStorePassword, keyManagerPassword,
                trustAll, nextProtocol);
        }
    }
}

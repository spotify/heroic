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
import com.google.common.collect.ImmutableList;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class JettyServerConnector {
    private final Optional<InetSocketAddress> address;
    private final List<JettyConnectionFactory> factories;
    private final Optional<String> defaultProtocol;
    private final JettyHttpConfiguration config;

    public static List<JettyConnectionFactory.Builder> defaultFactories() {
        return ImmutableList.of(HttpJettyConnectionFactory.builder(),
            Http2CJettyConnectionFactory.builder());
    }

    public ServerConnector setup(final Server server, final InetSocketAddress address) {
        final HttpConfiguration config = this.config.build();

        final ConnectionFactory[] factories = this.factories
            .stream()
            .map(f -> f.setup(config))
            .toArray(size -> new ConnectionFactory[size]);

        final ServerConnector c = new ServerConnector(server, factories);

        c.setHost(this.address.map(a -> a.getHostString()).orElseGet(address::getHostString));
        c.setPort(this.address.map(a -> a.getPort()).orElseGet(address::getPort));

        defaultProtocol.ifPresent(c::setDefaultProtocol);
        return c;
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder {
        private Optional<InetSocketAddress> address = Optional.empty();
        private Optional<List<JettyConnectionFactory.Builder>> factories = Optional.empty();
        private Optional<String> defaultProtocol = Optional.empty();
        private Optional<JettyHttpConfiguration.Builder> config = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("address") Optional<InetSocketAddress> address,
            @JsonProperty("factories") Optional<List<JettyConnectionFactory.Builder>> facts,
            @JsonProperty("defaultProtocol") Optional<String> defaultProtocol
        ) {
            this.address = address;
            this.factories = facts;
            this.defaultProtocol = defaultProtocol;
        }

        public Builder address(final InetSocketAddress address) {
            this.address = Optional.of(address);
            return this;
        }

        public Builder factories(final List<JettyConnectionFactory.Builder> factories) {
            this.factories = Optional.of(factories);
            return this;
        }

        public JettyServerConnector build() {
            final List<JettyConnectionFactory> factories = ImmutableList.copyOf(this.factories
                .orElseGet(JettyServerConnector::defaultFactories)
                .stream()
                .map(f -> f.build())
                .iterator());

            return new JettyServerConnector(address, factories, defaultProtocol,
                config.orElseGet(JettyHttpConfiguration::builder).build());
        }
    }
}

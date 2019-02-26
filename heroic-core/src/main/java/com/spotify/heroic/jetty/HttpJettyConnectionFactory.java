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
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;

public class HttpJettyConnectionFactory implements JettyConnectionFactory {

    public HttpJettyConnectionFactory() {
    }

    @Override
    public ConnectionFactory setup(final HttpConfiguration config) {
        return new HttpConnectionFactory(config);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements JettyConnectionFactory.Builder {
        @JsonCreator
        public Builder() {
        }

        @Override
        public JettyConnectionFactory build() {
            return new HttpJettyConnectionFactory();
        }
    }
}

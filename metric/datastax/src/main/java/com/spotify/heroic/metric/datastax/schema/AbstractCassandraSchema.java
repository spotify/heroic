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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.spotify.heroic.metric.datastax.Async;
import com.spotify.heroic.metric.datastax.ManagedSetupConnection;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

@RequiredArgsConstructor
public class AbstractCassandraSchema {
    protected final AsyncFramework async;

    protected AsyncFuture<PreparedStatement> prepareTemplate(
        final Map<String, String> values, Session s, final String path
    ) throws IOException {
        return Async.bind(async, s.prepareAsync(loadTemplate(path, values)));
    }

    protected AsyncFuture<PreparedStatement> prepareAsync(
        final Map<String, String> values, Session s, final String cql
    ) {
        return Async.bind(async, s.prepareAsync(variables(cql, values)));
    }

    private String loadTemplate(final String path, final Map<String, String> values)
        throws IOException {
        final String string;
        final ClassLoader loader = ManagedSetupConnection.class.getClassLoader();

        try (final InputStream is = loader.getResourceAsStream(path)) {
            if (is == null) {
                throw new IOException("No such resource: " + path);
            }

            string = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
        }

        return new StrSubstitutor(values, "{{", "}}").replace(string);
    }

    private String variables(String cql, Map<String, String> values) {
        return new StrSubstitutor(values, "{{", "}}").replace(cql);
    }
}

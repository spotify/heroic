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

package com.spotify.heroic.metric.bigtable.credentials;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;
import lombok.ToString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@ToString(of = {"path"})
public class JsonCredentialsBuilder implements CredentialsBuilder {
    public static final String DEFAULT_PATH = "./credentials.json";

    private final Path path;

    @JsonCreator
    public JsonCredentialsBuilder(@JsonProperty("path") Path path) {
        this.path = checkNotNull(path, "path");
    }

    @Override
    public CredentialOptions build() throws Exception {
        // XXX: You have to leave the input stream open for BigtableSession to use it.
        // This does 'leak' an input stream, but it's only once, so we'll live with it for now.
        // Reported here: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/issues/534
        return CredentialOptions.jsonCredentials(Files.newInputStream(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<Path> path = Optional.empty();

        public Builder path(Path path) {
            checkNotNull(path, "path");
            this.path = Optional.of(path);
            return this;
        }

        public JsonCredentialsBuilder build() {
            final Path p = path.orElse(Paths.get(DEFAULT_PATH));

            if (!Files.isReadable(p)) {
                throw new IllegalArgumentException("Path must be readable: " + p.toAbsolutePath());
            }

            return new JsonCredentialsBuilder(p);
        }
    }
}

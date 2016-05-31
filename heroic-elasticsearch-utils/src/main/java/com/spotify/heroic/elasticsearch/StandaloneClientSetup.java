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

package com.spotify.heroic.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StandaloneClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "heroic-standalone";

    private final String clusterName;
    private final Path root;

    @JsonCreator
    public StandaloneClientSetup(
        @JsonProperty("clusterName") String clusterName, @JsonProperty("root") String root
    ) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.root = checkDataDirectory(
            Paths.get(Optional.fromNullable(root).or(temporaryDirectory())).toAbsolutePath());
    }

    private Path checkDataDirectory(Path path) {
        if (!Files.isDirectory(path)) {
            throw new RuntimeException("No such directory: " + path.toAbsolutePath());
        }

        return path;
    }

    private String temporaryDirectory() {
        try {
            final File temp =
                File.createTempFile("heroic-elasticsearch", Long.toString(System.nanoTime()));

            if (!temp.delete()) {
                throw new RuntimeException("Could not delete temp file: " + temp.getAbsolutePath());
            }

            if (!temp.mkdir()) {
                throw new RuntimeException(
                    "Could not create temp directory: " + temp.getAbsolutePath());
            }

            return temp.getAbsolutePath();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create temporary directory", e);
        }
    }

    @Override
    public ClientWrapper setup() throws Exception {
        final Settings settings = Settings
            .builder()
            .put("path.home", root)
            .put("node.name", InetAddress.getLocalHost().getHostName())
            .put("script.inline", "on")
            // .put("script.disable_dynamic", false)
            // .put("script.groovy.sandbox.enabled",
            // true)
            .put("discovery.zen.ping.multicast.enabled", false)
            .build();
        final Node node = NodeBuilder
            .nodeBuilder()
            .settings(settings)
            .clusterName(clusterName)
            .node();
        return new ClientWrapper(node.client(), node::close);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clusterName;
        private String root;

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder root(String root) {
            this.root = root;
            return this;
        }

        public StandaloneClientSetup build() {
            return new StandaloneClientSetup(clusterName, root);
        }
    }
}

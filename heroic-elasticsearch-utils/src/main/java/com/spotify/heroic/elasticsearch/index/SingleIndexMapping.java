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

package com.spotify.heroic.elasticsearch.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SingleIndexMapping implements IndexMapping {
    private static final String DEFAULT_INDEX = "heroic";

    private final String index;
    private final String[] indices;
    private final Map<String, Object> settings;

    @JsonCreator
    public SingleIndexMapping(
        @JsonProperty("index") Optional<String> index,
        @JsonProperty("settings") Optional<Map<String, Object>> settings
    ) {
        this.index = index.orElse(DEFAULT_INDEX);
        this.indices = new String[]{this.index};
        this.settings = settings.orElse(new HashMap<>());
    }

    @Override
    public String template() {
        return index;
    }

    @Override
    public Map<String, Object> settings() {
        return settings;
    }

    @Override
    public String[] readIndices(String type) {
        return new String[]{getFullIndexName(type)};
    }

    @Override
    public String[] writeIndices(String type) {
        return new String[]{getFullIndexName(type)};
    }

    @Override
    public SearchRequestBuilder search(final Client client, final String type) {
        return client.prepareSearch(getFullIndexName(type));
    }

    @Override
    public SearchRequestBuilder count(final Client client, final String type) {
        return client
            .prepareSearch(getFullIndexName(type))
            .setSource(new SearchSourceBuilder().size(0));
    }

    @Override
    public List<DeleteRequestBuilder> delete(
        final Client client, final String type, final String id
    ) {
        return ImmutableList.of(client.prepareDelete(getFullIndexName(type), type, id));
    }

    private String getFullIndexName(String type) {
        return index + "-" + type;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toString() {
        return "SingleIndexMapping(index=" + this.index + ", indices=" + java.util.Arrays
            .deepToString(this.indices) + ")";
    }

    public static class Builder {
        private Optional<String> index = Optional.empty();
        private Optional<Map<String, Object>> settings = Optional.empty();

        public Builder index(String index) {
            this.index = Optional.of(index);
            return this;
        }

        public Builder settings(Map<String, Object> settings) {
            this.settings = Optional.of(settings);
            return this;
        }

        public SingleIndexMapping build() {
            return new SingleIndexMapping(index, settings);
        }
    }
}

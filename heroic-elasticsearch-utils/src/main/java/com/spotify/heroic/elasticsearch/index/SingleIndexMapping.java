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
import java.util.List;
import java.util.Optional;
import lombok.ToString;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

@ToString
public class SingleIndexMapping implements IndexMapping {
    public static final String DEFAULT_INDEX = "heroic";

    private final String index;
    private final String[] indices;

    @JsonCreator
    public SingleIndexMapping(@JsonProperty("index") Optional<String> index) {
        this.index = index.orElse(DEFAULT_INDEX);
        this.indices = new String[]{this.index};
    }

    public static SingleIndexMapping createDefault() {
        return new SingleIndexMapping(Optional.empty());
    }

    @Override
    public String template() {
        return index;
    }

    @Override
    public String[] readIndices() {
        return indices;
    }

    @Override
    public String[] writeIndices() {
        return indices;
    }

    @Override
    public SearchRequestBuilder search(final Client client, final String type) {
        return client.prepareSearch(index).setTypes(type);
    }

    @Override
    public CountRequestBuilder count(final Client client, final String type) {
        return client.prepareCount(index).setTypes(type);
    }

    @Override
    public List<DeleteRequestBuilder> delete(
        final Client client, final String type, final String id
    ) throws NoIndexSelectedException {
        return ImmutableList.of(client.prepareDelete(index, type, id));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> index = Optional.empty();

        public Builder index(String index) {
            this.index = Optional.of(index);
            return this;
        }

        public SingleIndexMapping build() {
            return new SingleIndexMapping(index);
        }
    }
}

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

package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.model.DataPoint;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(ResultGroup.DataPointResultGroup.class)})
public interface ResultGroup {

    public <T> List<T> valuesFor(final Class<T> expected);
    public List<TagValues> getTags();
    public List<?> getValues();

    @JsonIgnore
    public Class<?> getType();

    /**
     * TODO change the type string from a class name
     * @author mehrdad
     *
     */
    @Data
    @JsonTypeName("com.spotify.heroic.model.DataPoint")
    static class DataPointResultGroup implements ResultGroup {
        private final List<TagValues> tags;
        private final List<DataPoint> values;
        private final Class<DataPoint> type = DataPoint.class;

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> valuesFor(final Class<T> expected) {
            if (!expected.isAssignableFrom(type))
                throw new RuntimeException(String.format("incompatible payload type between %s (expected) and %s (actual)",
                        expected.getCanonicalName(), type.getCanonicalName()));

            return (List<T>) values;
        }

        @JsonCreator
        public DataPointResultGroup(@JsonProperty("tags") final List<TagValues> tags, @JsonProperty("values") final List<DataPoint> values) {
            this.tags = tags;
            this.values = values;
        }
    }
}

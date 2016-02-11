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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Backwards compatibility from the dark ages, where group aggregations 'each' field took a list.
 * <p>
 * XXX: remove when deprecated.
 *
 * @author udoprog
 */
@JsonDeserialize(using = AggregationOrList.Deserializer.class)
public class AggregationOrList {
    private final Optional<Aggregation> aggregation;

    public AggregationOrList(final Optional<Aggregation> aggregation) {
        this.aggregation = aggregation;
    }

    @JsonValue
    public Optional<Aggregation> toAggregation() {
        return aggregation;
    }

    public static final class Deserializer extends JsonDeserializer<AggregationOrList> {
        private static final TypeReference<List<Aggregation>> LIST_OF_AGGREGATIONS =
            new TypeReference<List<Aggregation>>() {
            };

        @Override
        public AggregationOrList deserialize(final JsonParser p, final DeserializationContext c)
            throws IOException, JsonProcessingException {
            switch (p.getCurrentToken()) {
                case START_ARRAY:
                    final List<Aggregation> chain = p.readValueAs(LIST_OF_AGGREGATIONS);
                    return new AggregationOrList(Aggregations.chain(chain));
                case START_OBJECT:
                    return new AggregationOrList(Optional.of(p.readValueAs(Aggregation.class)));
                default:
                    throw c.wrongTokenException(p, JsonToken.START_OBJECT, null);
            }
        }
    }
}

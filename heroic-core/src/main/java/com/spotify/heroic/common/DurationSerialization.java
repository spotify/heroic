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

package com.spotify.heroic.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ValueNode;

public class DurationSerialization {
    public static class Deserializer extends JsonDeserializer<Duration> {
        @Override
        public Duration deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            /* fallback to default parser if object */
            if (p.getCurrentToken() == JsonToken.START_OBJECT) {
                return deserializeObject(p.readValueAsTree(), c);
            }

            if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
                try {
                    return Duration.parseDuration(p.getValueAsString());
                } catch (Exception e) {
                    throw c.instantiationException(Duration.class, e);
                }
            }

            if (p.getCurrentToken().isNumeric()) {
                return new Duration(p.getLongValue(), Duration.DEFAULT_UNIT);
            }

            throw c.mappingException("Cannot deserialize Duration from input");
        }

        private Duration deserializeObject(TreeNode tree, DeserializationContext c)
                throws JsonMappingException {
            if (tree == null) {
                throw c.mappingException("expected object");
            }

            TreeNode node;
            ValueNode valueNode;

            final long duration;
            final TimeUnit unit;

            if ((node = tree.get("duration")) != null && node.isValueNode()
                    && (valueNode = (ValueNode) node).isNumber()) {
                duration = valueNode.asLong();
            } else {
                throw c.mappingException("duration is not a numeric field");
            }

            if ((node = tree.get("unit")) != null && node.isValueNode()
                    && (valueNode = (ValueNode) node).isTextual()) {
                unit = TimeUnit.valueOf(valueNode.asText().toUpperCase());
            } else {
                unit = Duration.DEFAULT_UNIT;
            }

            return new Duration(duration, unit);
        }
    }
}

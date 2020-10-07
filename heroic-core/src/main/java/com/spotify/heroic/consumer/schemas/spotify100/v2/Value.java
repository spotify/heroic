/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.consumer.schemas.spotify100.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;

/**
 * Distribution histogram point value. Currently we
 * support {@link Value.DoubleValue} and {@link Value.DistributionValue}
 */
@JsonSerialize(using = ValueSerializer.class)
@JsonDeserialize(using = ValueDeserializer.class)
public  abstract class Value {
    @JsonProperty("value")
    public abstract Object getValue();

    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static class DoubleValue extends Value {

        @Override
        public abstract Double  getValue();
        @JsonCreator
        public static Value.DoubleValue create(
            final double value) {
            return new AutoValue_Value_DoubleValue(value);
        }
    }

    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    public abstract static  class DistributionValue extends Value {

        @JsonCreator
        public static Value.DistributionValue create(
            final ByteString value) {
            return new AutoValue_Value_DistributionValue(value);
        }

        @Override
        public abstract ByteString getValue();
    }

}

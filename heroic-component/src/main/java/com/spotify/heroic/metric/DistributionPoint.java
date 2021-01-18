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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.hash.Hasher;
import org.jetbrains.annotations.NotNull;

@JsonSerialize(using = DistributionPointSerializer.class)
@JsonDeserialize(using = DistributionPointDeserialize.class)
@AutoValue
public abstract class DistributionPoint implements Metric {

    public abstract long getTimestamp();
    public abstract Distribution value();

    public static DistributionPoint create(final Distribution value, final long timestamp) {
        return new AutoValue_DistributionPoint(timestamp, value);
    }

    @Override
    public boolean valid() {
        return value().getValue() != null &&  !value().getValue().isEmpty();
    }

    @Override
    public void hash(@NotNull Hasher hasher) {
        hasher.putInt(this.hashCode());
    }
}

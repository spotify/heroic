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

package com.spotify.heroic.test;

import java.util.HashSet;
import java.util.Set;

/**
 * Simple class to encapsulate conditional logic on whether a key, tag or tag value should be
 * prepended with a [uniquely-identifying] timestamp.
 */
public class TimestampPrepender {

    private Set<EntityType> ets;
    private long timestamp;
    public TimestampPrepender(EntityType et, long timestamp) {
        this.ets = new HashSet<EntityType>();
        this.ets.add(et);
        this.timestamp = timestamp;
    }

    public TimestampPrepender(Set<EntityType> et, long timestamp) {
        this.ets = et;
        this.timestamp = timestamp;
    }

    public static String prepend(long timestamp, String input) {
        return Long.toString(timestamp) + "-" + input;
    }

    public String prepend(String input, EntityType et) {
        return this.ets.contains(et)
                ? prepend(timestamp, input)
                : input;
    }

    public enum EntityType {
        KEY,
        TAG,
        TAG_VALUE
    }
}

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

import com.google.common.hash.Hasher;

import java.util.Comparator;

public interface Metric {
    long getTimestamp();

    boolean valid();

    void hash(Hasher hasher);

    static Comparator<Metric> comparator() {
        return comparator;
    }

    static Metric invalid() {
        return invalid;
    }

    Comparator<Metric> comparator = Comparator.comparingLong(Metric::getTimestamp);

    Metric invalid = new Metric() {
        @Override
        public long getTimestamp() {
            throw new IllegalStateException("invalid has not timestamp");
        }

        @Override
        public boolean valid() {
            return false;
        }

        @Override
        public void hash(final Hasher hasher) {
        }
    };
}

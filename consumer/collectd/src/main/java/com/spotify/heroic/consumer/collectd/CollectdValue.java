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

package com.spotify.heroic.consumer.collectd;

import com.spotify.heroic.consumer.collectd.CollectdTypes.Field;
import lombok.Data;

public interface CollectdValue {
    public double convert(final CollectdTypes.Field field);

    public double toDouble();

    @Data
    public static class Gauge implements CollectdValue {
        private final double value;

        @Override
        public double convert(final Field field) {
            return field.convertGauge(value);
        }

        @Override
        public double toDouble() {
            return value;
        }
    }

    @Data
    public static class Absolute implements CollectdValue {
        private final long value;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(value);
        }

        @Override
        public double toDouble() {
            return value;
        }
    }

    @Data
    public static class Derive implements CollectdValue {
        private final long derivate;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(derivate);
        }

        @Override
        public double toDouble() {
            return derivate;
        }
    }

    @Data
    public static class Counter implements CollectdValue {
        private final long counter;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(counter);
        }

        @Override
        public double toDouble() {
            return counter;
        }
    }
}

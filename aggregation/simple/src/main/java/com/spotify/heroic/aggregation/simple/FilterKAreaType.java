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

package com.spotify.heroic.aggregation.simple;

import java.util.Comparator;

public enum FilterKAreaType {
    // @formatter:off
    TOP(((Comparator<Double>) Double::compare).reversed()),
    BOTTOM(Double::compare);
    // @formatter:on

    private final Comparator<Double> comparator;

    FilterKAreaType(Comparator<Double> comparator) {
        this.comparator = comparator;
    }

    public int compare(double a, double b) {
        return comparator.compare(a, b);
    }
}

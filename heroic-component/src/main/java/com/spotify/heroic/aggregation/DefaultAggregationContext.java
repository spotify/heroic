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

import com.google.common.base.Optional;

public class DefaultAggregationContext implements AggregationContext {
    final long defaultSize;

    public DefaultAggregationContext(final long defaultSize) {
        this.defaultSize = defaultSize;
    }

    @Override
    public Optional<Long> size() {
        return Optional.absent();
    }

    @Override
    public Optional<Long> extent() {
        return Optional.absent();
    }

    @Override
    public long defaultSize() {
        return defaultSize;
    }

    @Override
    public long defaultExtent() {
        return defaultSize();
    }
}
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

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Data;

public interface BackendKeyCriteria {
    public static BackendKeyCriteria all() {
        return new All();
    }

    public static BackendKeyCriteria and(final Iterable<BackendKeyCriteria> criterias) {
        return new And(ImmutableList.copyOf(criterias));
    }

    public static BackendKeyCriteria lt(final BackendKey key) {
        return new KeyLess(key);
    }

    public static BackendKeyCriteria gte(final BackendKey key) {
        return new KeyGreaterOrEqual(key);
    }

    public static BackendKeyCriteria gte(float percentage) {
        return new PercentageGereaterOrEqual(percentage);
    }

    public static BackendKeyCriteria lt(float percentage) {
        return new PercentageLess(percentage);
    }

    public static BackendKeyCriteria limited(BackendKeyCriteria criteria, int limit) {
        return new Limited(criteria, limit);
    }

    @Data
    class All implements BackendKeyCriteria {
    }

    @Data
    class Limited implements BackendKeyCriteria {
        private final BackendKeyCriteria criteria;
        private final int limit;
    }

    @Data
    class KeyGreaterOrEqual implements BackendKeyCriteria {
        private final BackendKey key;
    }

    @Data
    class KeyLess implements BackendKeyCriteria {
        private final BackendKey key;
    }

    @Data
    class PercentageGereaterOrEqual implements BackendKeyCriteria {
        private final float percentage;
    }

    @Data
    class PercentageLess implements BackendKeyCriteria {
        private final float percentage;
    }

    @Data
    class And implements BackendKeyCriteria {
        private final List<BackendKeyCriteria> criterias;
    }
}

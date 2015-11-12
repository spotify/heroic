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

import java.util.concurrent.atomic.AtomicLong;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LimitedFetchQuotaWatcher implements FetchQuotaWatcher {
    private final long dataLimit;

    private final AtomicLong read = new AtomicLong();

    @Override
    public boolean readData(long n) {
        final long r = read.addAndGet(n);
        return r < dataLimit;
    }

    @Override
    public boolean mayReadData() {
        return read.get() < dataLimit;
    }

    @Override
    public int getReadDataQuota() {
        final long left = dataLimit - read.get();

        if (left < 0) {
            return 0;
        }

        if (left > Integer.MAX_VALUE) {
            throw new IllegalStateException("quota too large");
        }

        return (int) left;
    }

    @Override
    public boolean isQuotaViolated() {
        return !mayReadData();
    }
}

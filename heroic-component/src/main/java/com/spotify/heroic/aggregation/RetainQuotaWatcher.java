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

public interface RetainQuotaWatcher {

    /**
     * Indicates that aggregation session has retained {@code n} more datapoints.
     *
     * @param n The number of datapoints retained by the aggregation session.
     * @throws com.spotify.heroic.common.QuotaViolationException if quota has been violated.
     */
    void retainData(long n);

    /**
     * Indicates if quota has been breached or not.
     *
     * @return {@code true} if there is data left to be retain, {@code false} otherwise.
     */
    boolean mayRetainMoreData();

    /**
     * Get how much data you are allowed to retain.
     */
    int getRetainQuota();

    /**
     * Special quota watcher indicating no quota should be applied.
     */
    RetainQuotaWatcher NO_QUOTA = new RetainQuotaWatcher() {

        @Override
        public void retainData(final long n) {
        }

        @Override
        public boolean mayRetainMoreData() {
            return true;
        }

        @Override
        public int getRetainQuota() {
            return Integer.MAX_VALUE;
        }
    };
}

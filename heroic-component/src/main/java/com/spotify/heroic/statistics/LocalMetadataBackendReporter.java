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

package com.spotify.heroic.statistics;

public interface LocalMetadataBackendReporter {
    FutureReporter.Context reportRefresh();

    FutureReporter.Context reportFindTags();

    FutureReporter.Context reportFindTagKeys();

    FutureReporter.Context reportFindTimeSeries();

    FutureReporter.Context reportCountSeries();

    FutureReporter.Context reportFindKeys();

    FutureReporter.Context reportWrite();

    void reportWriteCacheHit();

    void reportWriteCacheMiss();

    void reportWriteDroppedByRateLimit();

    /**
     * report number of successful operations in a batch
     *
     * @param n number of successes
     */
    void reportWriteSuccess(long n);

    /**
     * report number of failed operations in a batch
     *
     * @param n number of failures
     */
    void reportWriteFailure(long n);

    void reportWriteBatchDuration(long millis);

    void newWriteThreadPool(ThreadPoolProvider provider);

    ThreadPoolReporter newThreadPool();
}

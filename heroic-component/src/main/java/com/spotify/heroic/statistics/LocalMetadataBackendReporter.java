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
    public FutureReporter.Context reportRefresh();

    public FutureReporter.Context reportFindTags();

    public FutureReporter.Context reportFindTagKeys();

    public FutureReporter.Context reportFindTimeSeries();

    public FutureReporter.Context reportCountSeries();

    public FutureReporter.Context reportFindKeys();

    public FutureReporter.Context reportWrite();

    public void reportWriteCacheHit();

    public void reportWriteCacheMiss();

    public void reportWriteDroppedByRateLimit();

    /**
     * report number of successful operations in a batch
     *
     * @param n
     *            number of successes
     */
    public void reportWriteSuccess(long n);

    /**
     * report number of failed operations in a batch
     *
     * @param n
     *            number of failures
     */
    public void reportWriteFailure(long n);

    public void reportWriteBatchDuration(long millis);

    public void newWriteThreadPool(ThreadPoolProvider provider);

    public ThreadPoolReporter newThreadPool();
}

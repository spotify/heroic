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

package com.spotify.heroic.ingestion;

import com.spotify.heroic.common.BackendGroupException;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFuture;

public interface IngestionManager {
    public static final String INGESTED = "ingested";

    public AsyncFuture<WriteResult> write(String group, WriteMetric write) throws BackendGroupException;

    public Statistics getStatistics();

    /**
     * Configure a filter to use for ingestion.
     *
     * Any metric _not_ matching the filter will be dropped and instrumented accordingly.
     *
     * @param filter Filter to configure.
     * @return A future that resolved when the filter has been installed.
     */
    public AsyncFuture<Void> setFilter(Filter filter);

    public AsyncFuture<Filter> getFilter();
}
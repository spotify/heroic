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

package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;

public class NoopMetadataBackendReporter implements MetadataBackendReporter {
    private NoopMetadataBackendReporter() {
    }

    @Override
    public MetadataBackend decorate(
        final MetadataBackend backend
    ) {
        return backend;
    }

    @Override
    public void reportWriteDroppedByCacheHit() {
    }

    @Override
    public void reportWriteDroppedByDuplicate() {
    }

    @Override
    public FutureReporter.Context setupBackendWriteReporter() {
        return NoopFutureReporterContext.get();
    }

    private static final NoopMetadataBackendReporter instance = new NoopMetadataBackendReporter();

    public static NoopMetadataBackendReporter get() {
        return instance;
    }
}

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

package com.spotify.heroic.suggest;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.statistics.SuggestBackendReporter;

public interface SuggestModule {
    Exposed module(PrimaryComponent primary, Depends depends, String id);

    /**
     * Dependencies for suggestion modules.
     */
    class Depends {
        private final SuggestBackendReporter backendReporter;

        public Depends(final SuggestBackendReporter backendReporter) {
            this.backendReporter = backendReporter;
        }

        public SuggestBackendReporter getBackendReporter() {
            return backendReporter;
        }
    }

    /**
     * Exposed for suggestion modules.
     */
    interface Exposed {
        SuggestBackend backend();

        default LifeCycle life() {
            return LifeCycle.empty();
        }
    }
}

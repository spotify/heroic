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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Timer;
import com.spotify.heroic.statistics.HeroicTimer;

public class SemanticHeroicTimer implements HeroicTimer {

    @java.beans.ConstructorProperties({ "timer" })
    public SemanticHeroicTimer(final Timer timer) {
        this.timer = timer;
    }

    public String toString() {
        return "SemanticHeroicTimer()";
    }

    public class SemanticContext implements Context {
        private final Timer.Context context;

        @java.beans.ConstructorProperties({ "context" })
        public SemanticContext(final Timer.Context context) {
            this.context = context;
        }

        @Override
        public void finished() {
            stop();
        }

        @Override
        public long stop() {
            return context.stop();
        }
    }

    private final Timer timer;

    @Override
    public Context time() {
        return new SemanticContext(timer.time());
    }
}

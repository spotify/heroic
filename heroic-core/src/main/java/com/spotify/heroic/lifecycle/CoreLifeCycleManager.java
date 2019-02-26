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

package com.spotify.heroic.lifecycle;

import com.spotify.heroic.dagger.PrimaryScope;
import javax.inject.Inject;

@PrimaryScope
public class CoreLifeCycleManager implements LifeCycleManager {
    private final LifeCycleRegistry registry;

    @Inject
    public CoreLifeCycleManager(LifeCycleRegistry registry) {
        this.registry = registry;
    }

    @Override
    public LifeCycle build(final LifeCycles instance) {
        return build(instance.getClass().getCanonicalName(), instance);
    }

    @Override
    public LifeCycle build(final String id, final LifeCycles instance) {
        return new ManagedLifeCycle(id, instance);
    }

    class ManagedLifeCycle implements LifeCycle {
        private final String id;
        private final LifeCycles instance;

        public ManagedLifeCycle(final String id, final LifeCycles instance) {
            this.id = id;
            this.instance = instance;
        }

        @Override
        public void install() {
            instance.register(registry.scoped(id));
        }

        @Override
        public String toString() {
            return "+" + id;
        }
    }
}

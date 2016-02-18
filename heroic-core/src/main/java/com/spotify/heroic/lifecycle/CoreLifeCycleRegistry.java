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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.dagger.EarlyScope;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import org.eclipse.jetty.util.ConcurrentArrayQueue;

import javax.inject.Inject;
import java.util.List;
import java.util.Queue;

@EarlyScope
public class CoreLifeCycleRegistry implements LifeCycleRegistry {
    private final Queue<LifeCycleNamedHook<AsyncFuture<Void>>> starters =
        new ConcurrentArrayQueue<>();
    private final Queue<LifeCycleNamedHook<AsyncFuture<Void>>> stoppers =
        new ConcurrentArrayQueue<>();
    private final Queue<LifeCycleNamedHook<Boolean>> watchdogs = new ConcurrentArrayQueue<>();

    @Inject
    public CoreLifeCycleRegistry() {
    }

    @Override
    public void start(LifeCycleHook<AsyncFuture<Void>> starter) {
        start("<empty>", starter);
    }

    @Override
    public void stop(LifeCycleHook<AsyncFuture<Void>> stopper) {
        stop("<empty>", stopper);
    }

    @Override
    public void watch(final LifeCycleHook<Boolean> watchdog) {
        watch("<empty>", watchdog);
    }

    @Override
    public LifeCycleRegistry scoped(final String id) {
        return new LifeCycleRegistry() {
            @Override
            public void start(LifeCycleHook<AsyncFuture<Void>> starter) {
                CoreLifeCycleRegistry.this.start(id, starter);
            }

            @Override
            public void stop(LifeCycleHook<AsyncFuture<Void>> stopper) {
                CoreLifeCycleRegistry.this.stop(id, stopper);
            }

            @Override
            public void watch(final LifeCycleHook<Boolean> watchdog) {
                CoreLifeCycleRegistry.this.watch(id, watchdog);
            }

            @Override
            public LifeCycleRegistry scoped(final String add) {
                return CoreLifeCycleRegistry.this.scoped(id + "." + add);
            }
        };
    }

    public List<LifeCycleNamedHook<AsyncFuture<Void>>> starters() {
        return ImmutableList.copyOf(starters);
    }

    public List<LifeCycleNamedHook<AsyncFuture<Void>>> stoppers() {
        return ImmutableList.copyOf(stoppers);
    }

    public List<LifeCycleNamedHook<Boolean>> watchdogs() {
        return ImmutableList.copyOf(watchdogs);
    }

    private void start(final String id, final LifeCycleHook<AsyncFuture<Void>> starter) {
        starters.add(new Hook<>(id, starter));
    }

    private void stop(final String id, final LifeCycleHook<AsyncFuture<Void>> stopper) {
        stoppers.add(new Hook<>(id, stopper));
    }

    private void watch(final String id, final LifeCycleHook<Boolean> watchdog) {
        watchdogs.add(new Hook<>(id, watchdog));
    }

    @RequiredArgsConstructor
    static class Hook<T> implements LifeCycleNamedHook<T> {
        private final String id;
        private final LifeCycleHook<T> parent;

        @Override
        public T get() throws Exception {
            return parent.get();
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String toString() {
            return "/" + id;
        }
    }
}

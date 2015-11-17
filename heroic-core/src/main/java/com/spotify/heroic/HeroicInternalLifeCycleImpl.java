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

package com.spotify.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.inject.Injector;

@Slf4j
@RequiredArgsConstructor
public class HeroicInternalLifeCycleImpl implements HeroicInternalLifeCycle {
    private final List<Runnable> shutdownHooks = new ArrayList<Runnable>();
    private final List<StartupHookRunnable> startupHooks = new LinkedList<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Object lock = new Object();

    private volatile Injector injector;

    private static interface StartupHookRunnable {
        public void run(Injector injector);
    }

    @Override
    public void registerShutdown(final String name, final ShutdownHook hook) {
        if (stopped.get()) {
            throw new IllegalStateException("lifecycle already stopped");
        }

        synchronized (lock) {
            if (stopped.get()) {
                throw new IllegalStateException("lifecycle already stopped");
            }

            shutdownHooks.add(new Runnable() {
                @Override
                public void run() {
                    log.info("Shutting down '{}'", name);
                    runShutdownHook(name, hook);
                }
            });
        }
    }

    @Override
    public void register(final String name, final StartupHook hook) {
        if (!registerHook(name, hook)) {
            runStartupHook(name, hook);
        }
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        final Collection<StartupHookRunnable> hooks;

        synchronized (lock) {
            hooks = new ArrayList<>(this.startupHooks);
            startupHooks.clear();
        }

        for (StartupHookRunnable hook : hooks) {
            hook.run(injector);
        }
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        final Collection<Runnable> hooks;

        synchronized (lock) {
            hooks = new ArrayList<>(this.shutdownHooks);
            startupHooks.clear();
        }

        for (Runnable hook : hooks) {
            hook.run();
        }
    }

    /**
     * Registers a startup hook, unless the lifecycle has already been started.
     *
     * @param name Name of the hook to register.
     * @param hook Hook to register.
     * @return {@code true} if the hook was registered, {@code false} otherwise.
     */
    boolean registerHook(String name, StartupHook hook) {
        if (started.get()) {
            return false;
        }

        synchronized (lock) {
            if (started.get()) {
                return false;
            }

            startupHooks.add(new StartupHookRunnable() {
                @Override
                public void run(final Injector injector) {
                    log.info("Starting up '{}'", name);
                    runStartupHook(name, hook);
                }
            });

            return true;
        }
    }

    /**
     * Run the given shutdown hook in a safe environment that catches and logs exceptions.
     *
     * @param name The name of the hook.
     * @param hook The hook to run.
     */
    void runShutdownHook(final String name, final ShutdownHook hook) {
        try {
            hook.onShutdown();
        } catch (final Exception e) {
            log.error("Failed to shut down '{}'", name, e);
        }
    }

    /**
     * Run the given startup hook in a safe environment that catches and logs exceptions.
     *
     * @param name The name of the hook.
     * @param hook The hook to run.
     */
    void runStartupHook(final String name, final StartupHook hook) {
        final Context context = new Context() {
            @Override
            public void registerShutdown(ShutdownHook hook) {
                HeroicInternalLifeCycleImpl.this.registerShutdown(name, hook);
            }
        };

        try {
            hook.onStartup(context);
        } catch (Exception e) {
            log.error("Failed to start up '{}'", name, e);
        }
    }
}

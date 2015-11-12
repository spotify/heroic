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

package com.spotify.heroic.scheduler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Slf4j
@RequiredArgsConstructor
@ToString(exclude = { "scheduler" })
public class DefaultScheduler implements Scheduler {
    private static final String UNKNOWN = "unknown";

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(10,
            new ThreadFactoryBuilder().setNameFormat("heroic-scheduler#%d").build());

    @Override
    public void periodically(long value, final TimeUnit unit, final Task task) {
        periodically(UNKNOWN, value, unit, task);
    }

    @Override
    public void periodically(final String name, final long value, final TimeUnit unit,
            final Task task) {
        final Runnable refreshCluster = new Runnable() {
            @Override
            public void run() {
                try {
                    task.run();
                } catch (InterruptedException e) {
                    log.debug("task interrupted");
                } catch (final Exception e) {
                    log.error("task '{}' failed", name, e);
                }

                scheduler.schedule(this, value, unit);
            }
        };

        scheduler.schedule(refreshCluster, value, unit);
    }

    @Override
    public void schedule(long value, TimeUnit unit, final Task task) {
        schedule(UNKNOWN, value, unit, task);
    }

    @Override
    public void schedule(final String name, long value, TimeUnit unit, final Task task) {
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    task.run();
                } catch (final Exception e) {
                    log.error("{} task failed", name, e);
                }
            }
        }, value, unit);
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();

        try {
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            log.error("Failed to shut down scheduled executor service in a timely manner");
        }
    }
}

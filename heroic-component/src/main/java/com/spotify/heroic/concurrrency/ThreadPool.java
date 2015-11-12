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

package com.spotify.heroic.concurrrency;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * An abstraction for the concept of having separate thread pools dedicated towards reading vs.
 * writing to separate filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
@ToString(exclude = { "executor", "context" })
public class ThreadPool {
    public static final int DEFAULT_THREADS = 20;
    public static final int DEFAULT_QUEUE_SIZE = 10000;

    public static ThreadPool create(AsyncFramework async, String name, ThreadPoolReporter reporter,
            Integer threads, Integer queueSize) {
        if (threads == null) {
            threads = DEFAULT_THREADS;
        }

        final int size;

        if (queueSize == null) {
            size = DEFAULT_QUEUE_SIZE;
        } else {
            size = queueSize;
        }

        final ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 5000,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(size, false));

        final ThreadPoolReporter.Context context =
                reporter.newThreadPoolContext(name, new ThreadPoolReporterProvider() {
                    @Override
                    public long getQueueSize() {
                        return executor.getQueue().size();
                    }

                    @Override
                    public long getQueueCapacity() {
                        return size - executor.getQueue().size();
                    }

                    @Override
                    public long getActiveThreads() {
                        return executor.getActiveCount();
                    }

                    @Override
                    public long getPoolSize() {
                        return executor.getPoolSize();
                    }

                    @Override
                    public long getCorePoolSize() {
                        return executor.getCorePoolSize();
                    }
                });

        return new ThreadPool(async, name, executor, context, threads);
    }

    private final AsyncFramework async;
    private final String name;
    private final ThreadPoolExecutor executor;
    private final ThreadPoolReporter.Context context;

    @Getter
    private final int threadPoolSize;

    public ExecutorService get() {
        return executor;
    }

    public AsyncFuture<Void> stop() {
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                executor.shutdown();

                try {
                    executor.awaitTermination(120, TimeUnit.SECONDS);
                    log.debug("Gracefully shut down executor");
                } catch (final InterruptedException e) {
                    final List<?> tasks = executor.shutdownNow();
                    throw new Exception(String.format(
                            "Failed to gracefully stop read executors (%d tasks(s) killed)",
                            tasks.size()), e);
                } finally {
                    context.stop();
                }

                return null;
            }
        });
    }
}

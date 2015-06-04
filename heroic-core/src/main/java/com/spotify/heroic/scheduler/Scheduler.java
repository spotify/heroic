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

import java.util.concurrent.TimeUnit;

public interface Scheduler {

    /**
     * Schedule a task to be executed periodically.
     *
     * Same as {@link Scheduler#periodically(String, long, TimeUnit, Task)} but with a default name.
     */
    void periodically(long value, TimeUnit unit, Task task);

    /**
     * Schedule a task to be executed periodically.
     *
     * The task will be scheduler to execute immediately once.
     *
     * @param name Name of the task to execute (helpful when troubleshooting).
     * @param value Time interval that the task should execute.
     * @param unit Unit of the time interval.
     * @param task Task to execute.
     */
    void periodically(String name, long value, TimeUnit unit, Task task);

    void schedule(long value, TimeUnit unit, Task task);

    /**
     * Schedule a task to be executed after the given timeout.
     *
     * @param name The name of the task.
     * @param value Time interval that the task should execute.
     * @param unit Unit of the time interval.
     * @param task Task to execute.
     */
    void schedule(String name, long value, TimeUnit unit, Task task);

    /**
     * Stop the scheduler.
     */
    void stop();
}

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

package com.spotify.heroic.shell;

import eu.toolchain.async.AsyncFuture;

public interface ShellTask {
    /**
     * Return a new instance of a parameters object.
     * <p>
     * This instance <em>must not</em> declare any final fields.
     *
     * @return A new instance of a parameters object.
     */
    TaskParameters params();

    /**
     * Run the current task.
     *
     * @param io I/O access object. All printing and reading of files should happen through this
     * object. Must only be used by one thread at a time.
     * @param params Decoded parameters object.
     * @return A future, that once resolved indicates that the task has finished.
     * @throws Exception If an error occurs.
     */
    AsyncFuture<Void> run(final ShellIO io, TaskParameters params) throws Exception;
}

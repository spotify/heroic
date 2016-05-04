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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * I/O indirection for shell tasks.
 * <p>
 * If tasks directly open handles, these will be opened in the context of which the task is running.
 * This is most likely a server, which is not what the shell users typically intends.
 * <p>
 * This interface introduces implementation of methods that work correctly, regardless of context.
 *
 * @author udoprog
 */
public interface ShellIO {
    /**
     * Open a file for reading.
     *
     * @param path Path of file to open.
     * @param options Options when opening file.
     * @return An InputStream associated with the open file.
     */
    InputStream newInputStream(Path path, StandardOpenOption... options) throws IOException;

    /**
     * Open a file for writing.
     *
     * @param path Path of file to open.
     * @param options Options when opening file.
     * @return An OutputStream associated with the open file.
     */
    OutputStream newOutputStream(Path path, StandardOpenOption... options) throws IOException;

    /**
     * Get the output stream for this task.
     *
     * @return The output strema for this task.
     */
    PrintWriter out();
}

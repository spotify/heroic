/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic

import com.spotify.heroic.shell.ShellIO
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Path

data class DirectShellIO(val out: PrintWriter?) : ShellIO {

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream {
        return Files.newInputStream(path)
    }

    @Throws(IOException::class)
    override fun newOutputStream(path: Path): OutputStream {
        return Files.newOutputStream(path)
    }

    override fun out(): PrintWriter? {
        return out
    }
}

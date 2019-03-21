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

import com.spotify.heroic.shell.AbstractShellTaskParams
import java.util.ArrayList
import org.kohsuke.args4j.Option

class ShellParameters : AbstractShellTaskParams() {
    @Option(name = "--server", usage = "Start shell as server (enables listen port)")
    val server = false

    @Option(name = "--shell-server", usage = "Start shell with shell server (enables remote connections)")
    val shellServer = false

    @Option(name = "--disable-backends", usage = "Start core without configuring backends")
    val disableBackends = false

    @Option(name = "--connect", usage = "Connect to a remote heroic server", metaVar = "<host>[:<port>]")
    val connect: String? = null

    @Option(name = "-X", usage = "Define an extra parameter", metaVar = "<key>=<value>")
    val parameters = ArrayList<String>()
}

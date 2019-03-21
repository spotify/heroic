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

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import java.util.*

class Parameters {
    @Option(name = "-P", aliases = ["--profile"], usage = "Activate a pre-defined profile instead of a configuration file. Profiles" +
            " are pre-defined configurations, useful for messing around with the " +
            "system.")
    var profiles = ArrayList<String>()

    @Option(name = "--port", usage = "Port number to bind to")
    var port: Int? = null

    @Option(name = "--host", usage = "Host to bind to")
    var host: String? = null

    @Option(name = "--id", usage = "Heroic identifier")
    var id: String? = null

    @Option(name = "-h", aliases = ["--help"], help = true, usage = "Display help.")
    var help: Boolean = false

    @Option(name = "--startup-ping", usage = "Send a JSON frame to the given URI containing information about this " + "host after it has started.")
    var startupPing: String? = null

    @Option(name = "--startup-id", usage = "Explicit id of a specific startup instance.")
    var startupId: String? = null

    @Option(name = "-X", usage = "Define an extra parameter", metaVar = "<key>=<value>")
    var parameters = ArrayList<String>()

    @Argument
    var extra = ArrayList<String>()
}

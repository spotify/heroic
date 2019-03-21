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

package com.spotify.heroic.shell.task.parameters

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import java.util.*

internal class DataMigrateParameters : KeyspaceBase() {
    @Option(name = "-f", aliases = ["--from"], usage = "Backend group to load data from", metaVar = "<group>")
    val from = Optional.empty<String>()

    @Option(name = "-t", aliases = ["--to"], usage = "Backend group to load data to", metaVar = "<group>")
    val to = Optional.empty<String>()

    @Option(name = "--page-limit", usage = "Limit the number metadata entries to fetch per page (default: 100)")
    val pageLimit = 100

    @Option(name = "--keys-paged", usage = "Use the high-level paging mechanism when streaming keys")
    val keysPaged = false

    @Option(name = "--keys-page-size", usage = "Use the given page-size when paging keys")
    val keysPageSize = 10

    @Option(name = "--fetch-size", usage = "Use the given fetch size")
    val fetchSize = Optional.empty<Int>()

    @Option(name = "--tracing", usage = "Trace the queries for more debugging when things go wrong")
    val tracing = false

    @Option(name = "--parallelism", usage = "The number of migration requests to send in parallel (default: 100)", metaVar = "<number>")
    val parallelism = Runtime.getRuntime().availableProcessors() * 4

    @Argument
    override val query = ArrayList<String>()
}

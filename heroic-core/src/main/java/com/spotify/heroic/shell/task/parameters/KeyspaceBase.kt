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

import com.spotify.heroic.common.OptionalLimit
import org.kohsuke.args4j.Option

abstract class KeyspaceBase : QueryParamsBase() {
    @Option(name = "--start", usage = "First key to operate on", metaVar = "<json>")
    var start: String? = null

    @Option(name = "--end", usage = "Last key to operate on (exclusive)", metaVar = "<json>")
    var end: String? = null

    @Option(name = "--start-percentage", usage = "First key to operate on in percentage", metaVar = "<int>")
    var startPercentage = -1

    @Option(name = "--end-percentage", usage = "Last key to operate on (exclusive) in percentage", metaVar = "<int>")
    var endPercentage = -1

    @Option(name = "--start-token", usage = "First token to operate on", metaVar = "<long>")
    var startToken: Long? = null

    @Option(name = "--end-token", usage = "Last token to operate on (exclusive)", metaVar = "<int>")
    var endToken: Long? = null

    @Option(name = "--limit", usage = "Limit the number keys to operate on", metaVar = "<int>")
    override var limit: OptionalLimit = OptionalLimit.empty()
}

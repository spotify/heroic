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

package com.spotify.heroic.cluster

import eu.toolchain.async.Transform
import java.net.URI
import java.util.*
import java.util.concurrent.CancellationException
import java.util.function.Consumer

/**
 * A container that contains information about a node update.
 */
interface Update {
    /**
     * Handle the current update.
     *
     * @param successful Consumer for an successful update, will be called if the update is
     * successful
     * @param error Consumer for a failed update, will be called if the update is failed.
     */
    fun handle(successful: Consumer<SuccessfulUpdate>, error: Consumer<FailedUpdate>)

    class ErrorTransform(val uri: URI, val node: ClusterNode?): Transform<Throwable, Update> {
        override fun transform(error: Throwable): Update {
            return FailedUpdate(uri, error, Optional.ofNullable(node))
        }
    }

    class CancellationTransform(val uri: URI): Transform<Void, Update> {
        override fun transform(result: Void?): Update {
            return FailedUpdate(uri, CancellationException(), Optional.empty())
        }
    }

    companion object {
        @JvmStatic
        fun error(uri: URI): Transform<Throwable, Update> {
            return ErrorTransform(uri, null)
        }

        @JvmStatic
        fun error(uri: URI, existingNode: ClusterNode): Transform<Throwable, Update> {
            return ErrorTransform(uri, existingNode)
        }

        @JvmStatic
        fun cancellation(uri: URI): Transform<Void, Update> {
            return CancellationTransform(uri)
        }
    }
}

/**
 * A successful node update.
 */
data class SuccessfulUpdate(
    // The URI that was updated.
    val uri: URI,
    // If the update is a new addition.
    val added: Boolean,
    // The cluster node part of the update.
    val node: ClusterNode
) : Update {
    fun isAdded(): Boolean = added

    override fun handle(successful: Consumer<SuccessfulUpdate>, error: Consumer<FailedUpdate>) {
        successful.accept(this)
    }
}

/**
 * A failed node update.
 */
data class FailedUpdate(
    // The URI of the node that failed to update.
    val uri: URI,
    // The error that caused the failure.
    val error: Throwable,
    // The existing node, to be closed
    val existingNode: Optional<ClusterNode>
) : Update {
    override fun handle(successful: Consumer<SuccessfulUpdate>, error: Consumer<FailedUpdate>) {
        error.accept(this)
    }
}
/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.usagetracking.google

import com.google.common.hash.Hashing
import com.spotify.heroic.scheduler.Scheduler
import com.spotify.heroic.usagetracking.UsageTracking
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Named

private val log: Logger = LoggerFactory.getLogger(GoogleAnalytics::class.java)

private val CLIENT = OkHttpClient()
private val BASE_REQUEST = Request.Builder()
    .url("https://www.google-analytics.com/collect")

class GoogleAnalytics @Inject constructor(
    @Named("version") val version: String,
    @Named("commit") val commit: String,
    val scheduler: Scheduler
): UsageTracking {
    private val runtimeBean = ManagementFactory.getRuntimeMXBean()
    private val clientId: String
    private val eventVersion = "$version:$commit"

    init {
        val hostname = lookupHostname()
        clientId = if (hostname == null) "UNKNOWN_HOST"
        else Hashing.sha256().hashString(hostname, Charsets.UTF_8).toString()

        scheduler.periodically(1, TimeUnit.HOURS, ::reportUptime)
    }

    override fun reportStartup() {
        val event = Event(clientId)
            .category("deployment")
            .action("startup")
            .version(eventVersion)
        sendEvent(event)
    }

    override fun reportClusterSize(size: Int) {
        val event = Event(clientId)
            .category("deployment")
            .action("cluster_refresh")
            .version(eventVersion)
            .value(size.toString())
        sendEvent(event)
    }

    /**
     * Report time, in milliseconds, since the JVM started.
     */
    private fun reportUptime() {
        val event = Event(clientId)
            .category("deployment")
            .action("uptime")
            .version(eventVersion)
            .value(runtimeBean.uptime.toString())
        sendEvent(event)
    }

    private fun sendEvent(event: Event): Response {
        val body = event.build()
        log.debug("Sending event to Google Analytics")

        val result = CLIENT.newCall(BASE_REQUEST.post(body).build()).execute()
        if (!result.isSuccessful) {
            log.error("Failed to send event to Google Analytics: {}", result.code())
        }
        return result
    }
}

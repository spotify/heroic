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

import okhttp3.FormBody

/**
 * Builder for Google Analytics events.
 *
 * Dev guide: https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide
 * Param reference: https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
 * Guide to events: https://support.google.com/analytics/answer/1033068?hl=en
 */
class Event(clientId: String, propertyId: String = "UA-48549177-5") {
    val builder: FormBody.Builder = FormBody.Builder()
        .add("v", "1")
        .add("tid", propertyId)
        .add("cid", clientId)
        .add("t", "event")
        .add("ni", "1")
        .add("an", "Heroic")
        .add("aiid", System.getProperty("java.runtime.version"))

    fun category(category: String): Event {
        builder.add("ec", category)
        return this
    }

    fun action(action: String): Event {
        builder.add("ea", action)
        return this
    }

    fun label(label: String): Event {
        builder.add("el", label)
        return this
    }

    fun value(value: String): Event {
        builder.add("ev", value)
        return this
    }

    fun version(version: String): Event {
        builder.add("av", version)
        return this
    }

    fun build(): FormBody = builder.build()
}

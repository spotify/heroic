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

import com.spotify.heroic.dagger.PrimaryComponent
import com.spotify.heroic.usagetracking.UsageTracking
import com.spotify.heroic.usagetracking.UsageTrackingComponent
import com.spotify.heroic.usagetracking.UsageTrackingModule
import dagger.Component
import dagger.Module
import dagger.Provides
import javax.inject.Named

class GoogleAnalyticsModule(val version: String, val commit: String): UsageTrackingModule {
    override fun module(primary: PrimaryComponent): UsageTrackingComponent {
        return DaggerGoogleAnalyticsModule_C
            .builder()
            .primaryComponent(primary)
            .m(M(version, commit))
            .build()
    }

    @UsageTrackingScope
    @Component(modules = [M::class], dependencies = [PrimaryComponent::class])
    interface C: UsageTrackingComponent {
        override fun usageTracking(): UsageTracking
    }

    @Module
    class M(val version: String, val commit: String) {
        @UsageTrackingScope
        @Provides
        fun usageTracking(googleAnalytics: GoogleAnalytics): UsageTracking {
            return googleAnalytics
        }

        @UsageTrackingScope
        @Provides
        @Named("version")
        fun version(): String = version

        @UsageTrackingScope
        @Provides
        @Named("commit")
        fun commit(): String = commit
    }

    class Builder: UsageTrackingModule.Builder {
        var version: String = ""
        var commit: String = ""

        override fun version(version: String, commit: String): Builder {
            this.version = version
            this.commit = commit
            return this
        }

        override fun build() = GoogleAnalyticsModule(version, commit)
    }
}

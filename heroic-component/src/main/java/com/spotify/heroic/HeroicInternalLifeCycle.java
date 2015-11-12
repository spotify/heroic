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

package com.spotify.heroic;

public interface HeroicInternalLifeCycle {
    interface Context {
        void registerShutdown(ShutdownHook hook);
    }

    interface ShutdownHook {
        public void onShutdown() throws Exception;
    }

    interface StartupHook {
        void onStartup(HeroicInternalLifeCycle.Context context) throws Exception;
    }

    /**
     * Register a hook to be run at shutdown.
     *
     * @param name
     * @param runnable
     */
    void registerShutdown(String name, ShutdownHook hook);

    /**
     * Register a hook to be run at startup.
     *
     * @param name
     * @param startup
     */
    void register(String name, StartupHook hook);

    void start();

    void stop();
}

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

package com.spotify.heroic.args4j;

import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.OptionalLimit;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerRegistry;

import java.util.Optional;

public class CmdLine {
    private static volatile boolean registeredHandlers = false;

    /**
     * Every user of {@link org.kohsuke.args4j.CmdLineParser} must use this method to create the
     * CmdLineParser to assert that custom handlers are registered before.
     *
     * @param params The params object to setup parser for.
     * @return A new {@link org.kohsuke.args4j.CmdLineParser} instance for the given params object.
     */
    public static CmdLineParser createParser(final Object params) {
        registerHandlersIfNeeded();
        return new CmdLineParser(params);
    }

    private static void registerHandlersIfNeeded() {
        if (registeredHandlers) {
            return;
        }

        synchronized (CmdLine.class) {
            if (registeredHandlers) {
                return;
            }

            OptionHandlerRegistry
                .getRegistry()
                .registerHandler(Duration.class, DurationOptionHandler.class);
            OptionHandlerRegistry
                .getRegistry()
                .registerHandler(Optional.class, OptionalOptionHandler.class);
            OptionHandlerRegistry
                .getRegistry()
                .registerHandler(OptionalLimit.class, OptionalLimitOptionHandler.class);

            registeredHandlers = true;
        }
    }
}

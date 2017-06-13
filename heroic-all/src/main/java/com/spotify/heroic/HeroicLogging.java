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

import org.slf4j.bridge.SLF4JBridgeHandler;

public class HeroicLogging {
    public static void configure() {
        configureNetty();
        configureJUL();
    }

    /**
     * Initialize all Netty logging consistently.
     * <p>
     * Netty decided it was a good idea to build their own logging framework. From this follows that
     * every time netty has been shaded, it needs to be initialized independently to properly
     * redirect all logging to SLF4j.
     */
    public static void configureNetty() {
        com.spotify.heroic.bigtable.netty.util.internal.logging.InternalLoggerFactory
            .setDefaultFactory(
            new com.spotify.heroic.bigtable.netty.util.internal.logging.Slf4JLoggerFactory());

        com.datastax.shaded.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(
            new com.datastax.shaded.netty.util.internal.logging.Slf4JLoggerFactory());

        io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(
            new io.netty.util.internal.logging.Slf4JLoggerFactory());

        org.jboss.netty.logging.InternalLoggerFactory.setDefaultFactory(
            new org.jboss.netty.logging.Slf4JLoggerFactory());
    }

    /**
     * Initialize java.util.logging to SLF4J.
     */
    public static void configureJUL() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }
}

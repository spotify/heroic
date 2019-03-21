/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.http.tracing;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import org.apache.commons.lang3.NotImplementedException;


public class OpenCensusFeature implements Feature {
    private final Verbosity verbosity;

    /**
     * Creates feature instance with default ({@link Verbosity#INFO} verbosity level.
     */
    public OpenCensusFeature() {
        verbosity = Verbosity.INFO;
    }

    /**
     * Creates feature instance with given ({@link Verbosity} level.
     * @param verbosity desired level of logging verbosity
     */
    public OpenCensusFeature(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    /**
     * Stored span's {@link ContainerRequestContext} property key.
     */
    public static final String SPAN_CONTEXT_PROPERTY = "span";

    @Override
    public boolean configure(FeatureContext context) {

        switch (context.getConfiguration().getRuntimeType()) {
            case CLIENT:
                throw new NotImplementedException("Client tracing not implemented");
            case SERVER:
                context.register(new OpenCensusApplicationEventListener(verbosity));
            default:
                // This can't happen.
        }
        return true;
    }

    /**
     * OpenCencsus Jersey event logging verbosity.
     */
    public enum Verbosity {
        /**
         * Only logs basic Jersey processing related events.
         */
        INFO,

        /**
         * Logs more fine grained events related to Jersey processing.
         */
        TRACE
    }
}

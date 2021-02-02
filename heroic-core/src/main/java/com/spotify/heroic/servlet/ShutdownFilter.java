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

package com.spotify.heroic.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.ws.ErrorMessage;
import com.spotify.heroic.ws.InternalErrorMessage;
import java.io.IOException;
import java.util.function.Supplier;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

public class ShutdownFilter extends SimpleFilter {
    private final Supplier<Boolean> stopping;

    public ShutdownFilter(final Supplier<Boolean> stopping, final ObjectMapper mapper) {
        super(mapper);
        this.stopping = stopping;
    }

    @Override
    public boolean passesFilter(ServletRequest request) {
        return !stopping.get();
    }

    @Override
    public ErrorMessage doFilterImpl(
            final HttpServletRequest request, final HttpServletResponse response,
            final FilterChain chain
    ) throws IOException, ServletException {
        final var info =
                new InternalErrorMessage("Heroic is shutting down", Status.SERVICE_UNAVAILABLE);

        response.setStatus(Status.SERVICE_UNAVAILABLE.getStatusCode());
        return info;
    }
};

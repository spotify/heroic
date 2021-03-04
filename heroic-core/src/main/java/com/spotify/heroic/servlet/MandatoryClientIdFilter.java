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
import com.google.common.base.Strings;
import com.spotify.heroic.ws.ErrorMessage;
import com.spotify.heroic.ws.MandatoryClientIdErrorMessage;
import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

/**
 * Rejects anonymous requests. That is, requests to any API endpoint that are
 * missing a non-null X-Client-Id HTTP header.<p></p>
 * *Note* that this @SuppressWarnings has to go here even though it's just for the doFilter
 * method, else the JavaDoc for it doesn't render. Weird.
 */
@SuppressWarnings("checkstyle:LineLength")
public class MandatoryClientIdFilter extends SimpleFilter {

    public static final String X_CLIENT_ID_HEADER_NAME = "X-Client-Id";
    public static final String MISSING_X_CLIENT_ID = "missing-client-id";

    public static final String ERROR_MESSAGE_TEXT =
            "This anonymous request has been rejected. Please add a 'x-client-id' " +
                    "HTTP header to your request.";

    public MandatoryClientIdFilter(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Reject (with a 400) the request, if the X-Client-Id HTTP header is not present
     * or is non-null/empty.<p>
     * Calling {@link javax.servlet.FilterChain#doFilter}
     * effectively "passes" this filter and the next
     * filter gets a stab at it. <p>
     * Conversely, not calling doFilter halts "happy path" processing altogether
     * and that's the mechanism with which we stop anonymous requests.<p>
     * Finally, using
     * {@link javax.servlet.http.HttpServletResponse#sendError(int, java.lang.String)} to
     * return a status message didn't work and instead sent text of "internal error" back
     * to the client.
     */
    @Override
    public ErrorMessage doFilterImpl(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain
    ) throws IOException, ServletException {

        final var info =
                new MandatoryClientIdErrorMessage("Anonymous requests are not permitted");

        response.setStatus(Status.BAD_REQUEST.getStatusCode());
        return info;
    }

    /**
     * Returns true if the HTTP header X-Client-Id is present and non-null and not empty.
     * @param request request to pluck X-Client-Id's value from
     * @return see above
     */
    @Override
    public boolean passesFilter(ServletRequest request) {
        var req = HttpServletRequest.class.cast(request);
        return !Strings.isNullOrEmpty(req.getHeader(X_CLIENT_ID_HEADER_NAME));
    }

};

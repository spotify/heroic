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

import com.google.common.base.Strings;
import com.spotify.heroic.common.MandatoryClientIdUtil.RequestInfractionSeverity;
import java.io.IOException;
import java.util.Optional;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rejects anonymous requests if {@link #severity} is set to
 * {@link RequestInfractionSeverity#REJECT}.
 * <p>
 * Otherwise if set to {@link RequestInfractionSeverity#WARNING}, the anonymous request is
 * permitted but a warning is added (via {@link com.spotify.heroic.metric.QueryError}) to
 * {@link com.spotify.heroic.metric.QueryMetricsResponse#errors}.
 * <p>
 * Finally, anonymous requests are permitted when it's set to
 * {@link RequestInfractionSeverity#PERMIT}.
 */
public class MandatoryClientIdFilter implements Filter {

    private static final Logger classLogger =
        LoggerFactory.getLogger(MandatoryClientIdFilter.class);

    private final Logger logger;

    private final RequestInfractionSeverity severity;

    public MandatoryClientIdFilter(RequestInfractionSeverity severity,
        Optional<Logger> parentLogger) {
        this.severity = severity;
        this.logger = parentLogger.orElse(classLogger);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    /**
     * Reject (400) or pass (200) the request, depending on our config and the
     * X-Client-Id HTTP header's contents.
     */
    @Override
    public void doFilter(
        ServletRequest request, ServletResponse response, FilterChain chain
    ) throws IOException, ServletException {

        try {
            if (passesFilter(severity, request)) {
                return;
            }

            var httpResponse = (HttpServletResponse) response;

            var httpStatus = severity ==
                RequestInfractionSeverity.REJECT ? Status.BAD_REQUEST : Status.OK;

            httpResponse.setStatus(httpStatus.getStatusCode());
        } finally {
            chain.doFilter(request, response);
        }
    }

    /**
     * Assuming {@link #severity} is not PERMIT, returns true if the HTTP header
     * X-Client-Id is present and non-null and not empty.
     * @param severity severity to consider
     * @param request request to pluck X-Client-Id's value from
     * @return see above
     */
    public static boolean passesFilter(RequestInfractionSeverity severity, ServletRequest request) {
        if (severity == RequestInfractionSeverity.PERMIT) {
            return true;
        }

        // TODO don't think this test is necessary but keep it around until testing proves as amuch
//        if (request.getClass().isAssignableFrom(HttpServletRequest.class)) {
        var req = HttpServletRequest.class.cast(request);
        return  !Strings.isNullOrEmpty(req.getHeader("X-Client-Id"));
    }

    @Override
    public void destroy() {
    }
};

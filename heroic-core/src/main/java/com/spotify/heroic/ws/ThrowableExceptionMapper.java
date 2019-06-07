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

package com.spotify.heroic.ws;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;

public class ThrowableExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger log =
        org.slf4j.LoggerFactory.getLogger(ThrowableExceptionMapper.class);

    @Inject
    public ThrowableExceptionMapper() {
    }

    @Override
    public Response toResponse(Throwable e) {
        log.error("Internal Error", e);

        return Response
            .status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(new InternalErrorMessage(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }
}

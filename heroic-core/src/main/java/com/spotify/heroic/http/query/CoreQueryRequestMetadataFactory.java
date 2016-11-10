/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic.http.query;

import com.spotify.heroic.QueryRequestMetadata;
import javax.servlet.http.HttpServletRequest;

public class CoreQueryRequestMetadataFactory {

    public static QueryRequestMetadata create(HttpServletRequest httpServletRequest) {
        String userAgent = httpServletRequest.getHeader("User-Agent");
        String clientId = httpServletRequest.getHeader("X-Alient-Client-Id");
        return new QueryRequestMetadata(httpServletRequest.getRemoteAddr(),
                                        httpServletRequest.getRemoteHost(),
                                        httpServletRequest.getRemotePort(),
                                        userAgent == null ? "" : userAgent,
                                        clientId == null ? "" : clientId);
    }

}

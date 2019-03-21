/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.http


import java.io.IOException
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.container.ContainerResponseContext
import javax.ws.rs.container.ContainerResponseFilter

data class CorsResponseFilter(val corsAllowOrigin: String) : ContainerResponseFilter {

    @Throws(IOException::class)
    override fun filter(
            requestContext: ContainerRequestContext, responseContext: ContainerResponseContext
    ) {
        val headers = responseContext.headers

        if (requestContext.method.equals("OPTIONS", ignoreCase = true)) {
            val request: String? = requestContext.getHeaderString("Access-Control-Request-Headers")
            val method: String? = requestContext.getHeaderString("Access-Control-Request-Method")

            if (request == null || method == null) {
                return
            }

            headers.add("Access-Control-Allow-Origin", corsAllowOrigin)
            headers.add("Access-Control-Allow-Methods", method)
            headers.add("Access-Control-Allow-Headers", request)
        } else {
            headers.add("Access-Control-Allow-Origin", corsAllowOrigin)
        }
    }
}

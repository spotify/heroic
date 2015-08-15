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

package com.spotify.heroic.httpclient;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.spotify.heroic.http.ErrorMessage;

public class HttpClientUtils {

    public static <T> T handleResponse(Response response, Class<T> bodyType, WebTarget target)
            throws RpcRemoteException {
        final String contentType = response.getHeaderString(HttpHeaders.CONTENT_TYPE);

        if (contentType == null) {
            throw new RpcRemoteException(target.getUri(), "No Content-Type in response",
                    response.readEntity(String.class));
        }

        if (response.getStatusInfo().getFamily() != Status.Family.SUCCESSFUL) {
            if (contentType.equals(MediaType.APPLICATION_JSON)) {
                final ErrorMessage error = response.readEntity(ErrorMessage.class);
                throw new RpcRemoteException(target.getUri(), error.getMessage());
            }

            final String entity = response.readEntity(String.class);
            throw new RpcRemoteException(target.getUri(), "Error from request", entity);
        }

        if (!contentType.equals(MediaType.APPLICATION_JSON)) {
            throw new RpcRemoteException(target.getUri(), "Got body of unexpected Content-Type: " + contentType,
                    response.readEntity(String.class));
        }

        return response.readEntity(bodyType);
    }
}

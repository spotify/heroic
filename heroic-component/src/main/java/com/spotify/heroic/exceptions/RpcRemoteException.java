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

package com.spotify.heroic.exceptions;

import java.net.URI;

import lombok.Getter;

public class RpcRemoteException extends RpcNodeException {
    private static final long serialVersionUID = 4669248130340709248L;

    @Getter
    private final String entity;

    public RpcRemoteException(URI uri, String message) {
        super(uri, message);
        this.entity = null;
    }

    public RpcRemoteException(URI uri, String message, String entity) {
        super(uri, formatMessage(message, entity));
        this.entity = entity;
    }

    private static String formatMessage(String message, Object entity) {
        final StringBuilder builder = new StringBuilder();
        builder.append(message).append(", entity: ").append(entity.toString());
        return builder.toString();
    }
}

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

package com.spotify.heroic.rpc.nativerpc;

import java.net.InetSocketAddress;

import lombok.Getter;
import lombok.ToString;

@ToString(of = { "address", "message" })
public class NativeRpcRemoteException extends Exception {
    private static final long serialVersionUID = -664905544594225316L;

    @Getter
    public final InetSocketAddress address;

    public final String message;

    public NativeRpcRemoteException(InetSocketAddress address, String message) {
        super(address + ": " + message);
        this.address = address;
        this.message = message;
    }
}
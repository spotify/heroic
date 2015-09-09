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

package com.spotify.heroic;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.URI;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicInternalLifeCycle.Context;
import com.spotify.heroic.common.LifeCycle;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * Component that executes a startup 'ping' after the service has started.
 * 
 * This is used primarally to hook into integration tests to assert that the service has been configured before tests
 * are being executed.
 *
 * @author udoprog
 */
@Slf4j
@RequiredArgsConstructor
@ToString(of = { "ping", "id" })
public class HeroicStartupPinger implements LifeCycle {
    @Inject
    private HeroicServer server;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private HeroicInternalLifeCycle lifecycle;

    @Inject
    private AsyncFramework async;

    private final URI ping;
    private final String id;

    @Override
    public AsyncFuture<Void> start() {
        lifecycle.register("Startup Ping", new HeroicInternalLifeCycle.StartupHook() {
            @Override
            public void onStartup(Context context) throws Exception {
                log.info("Sending startup ping to {}", ping);
                final PingMessage ping = new PingMessage(server.getPort(), id);
                sendStartupPing(ping);
            }
        });

        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.resolved(null);
    }

    @Override
    public boolean isReady() {
        return true;
    }

    private void sendStartupPing(PingMessage p) throws IOException {
        switch (ping.getScheme()) {
        case "udp":
            sendUDP(p);
            break;
        default:
            throw new IllegalArgumentException("Startup URL scheme: " + ping.getScheme());
        }
    }

    private void sendUDP(PingMessage p) throws IOException {
        if (ping.getPort() == -1)
            throw new IllegalArgumentException("Invalid URI, port is required: " + ping);

        final byte[] frame = mapper.writeValueAsBytes(p);

        InetSocketAddress address = new InetSocketAddress(ping.getHost(), ping.getPort());
        final DatagramPacket packet = new DatagramPacket(frame, frame.length, address);

        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.send(packet);
        }
    }

    @Data
    public static final class PingMessage {
        private final int port;
        private final String id;
    }
}

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

package com.spotify.heroic.consumer.collectd;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.GrokProcessor;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public class CollectdChannelHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final AsyncFramework async;
    private final IngestionGroup ingestion;
    private final Optional<GrokProcessor> hostProcessor;
    private final CollectdTypes types;

    @java.beans.ConstructorProperties({ "async", "ingestion", "hostProcessor", "types" })
    public CollectdChannelHandler(final AsyncFramework async, final IngestionGroup ingestion,
                                  final Optional<GrokProcessor> hostProcessor,
                                  final CollectdTypes types) {
        this.async = async;
        this.ingestion = ingestion;
        this.hostProcessor = hostProcessor;
        this.types = types;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg)
        throws Exception {
        final Iterator<CollectdSample> samples = CollectdParser.parse(msg.content());

        while (samples.hasNext()) {
            final CollectdSample s = samples.next();

            final Set<Map.Entry<String, String>> base =
                ImmutableMap.of("host", s.getHost(), "plugin", s.getPlugin()).entrySet();

            final List<Ingestion.Request> ingestions;

            if (hostProcessor.isPresent()) {
                final Map<String, Object> parts = hostProcessor.get().parse(s.getHost());

                final Set<Map.Entry<String, String>> tags = ImmutableSet.copyOf(
                    Iterables.transform(parts.entrySet(),
                        e -> Pair.of(e.getKey(), e.getValue().toString())));

                ingestions = types.convert(s, Iterables.concat(base, tags));
            } else {
                ingestions = types.convert(s, base);
            }

            final List<AsyncFuture<Ingestion>> futures = new ArrayList<>();

            for (final Ingestion.Request w : ingestions) {
                futures.add(ingestion.write(w));
            }

            async.collectAndDiscard(futures);
        }
    }
}

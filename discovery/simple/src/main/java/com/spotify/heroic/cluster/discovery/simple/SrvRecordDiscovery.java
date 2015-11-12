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

package com.spotify.heroic.cluster.discovery.simple;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Named;

import org.xbill.DNS.DClass;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.Type;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterDiscovery;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@ToString
@Slf4j
public class SrvRecordDiscovery implements ClusterDiscovery {
    public static final String DEFAULT_PROTOCOL = "nativerpc";
    public static final int DEFAULT_PORT = 1394;

    @Inject
    private AsyncFramework async;

    @Inject
    @Named("records")
    private List<String> records;

    @Inject
    @Named("protocol")
    private Optional<String> protocol;

    @Inject
    @Named("port")
    private Optional<Integer> port;

    @Override
    public AsyncFuture<List<URI>> find() {
        final ImmutableList.Builder<AsyncFuture<List<URI>>> lookups = ImmutableList.builder();

        for (final String record : records) {
            lookups.add(async.call(() -> {
                log.info("Resolving SRV records for: {}", record);

                final Lookup lookup = new Lookup(record, Type.SRV, DClass.IN);

                final Record[] result = lookup.run();

                if (lookup.getResult() != Lookup.SUCCESSFUL) {
                    log.error("Failed to lookup record: {}: {}", record, lookup.getErrorString());
                    return ImmutableList.<URI> of();
                }

                final ImmutableList.Builder<URI> results = ImmutableList.builder();

                if (result != null) {
                    for (final Record a : result) {
                        final SRVRecord srv = (SRVRecord) a;
                        results.add(new URI(protocol.orElse(DEFAULT_PROTOCOL) + "://"
                                + srv.getTarget().canonicalize() + ":"
                                + port.orElse(DEFAULT_PORT)));
                    }
                }

                return results.build();
            }));
        }

        return async.collect(lookups.build()).directTransform(all -> {
            final ImmutableList.Builder<URI> results = ImmutableList.builder();
            all.forEach(results::addAll);
            return results.build();
        });
    }
}

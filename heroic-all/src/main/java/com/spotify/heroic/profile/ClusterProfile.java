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

package com.spotify.heroic.profile;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.cluster.discovery.simple.SrvRecordDiscoveryModule;
import com.spotify.heroic.cluster.discovery.simple.StaticListDiscoveryModule;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocolModule;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

import static com.spotify.heroic.ParameterSpecification.parameter;

public class ClusterProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final ClusterManagerModule.Builder module = ClusterManagerModule.builder();

        module.protocols(ImmutableList.of(NativeRpcProtocolModule.builder().build()));

        switch (params.get("discovery").orElse("static")) {
            case "static":
                final List<URI> nodes = ImmutableList.copyOf(
                    params.getAsList("host").stream().map(this::buildUri).iterator());
                module.discovery(new StaticListDiscoveryModule(nodes));
                break;
            case "srv":
                final List<String> records = params.getAsList("record");
                final SrvRecordDiscoveryModule.Builder sd =
                    SrvRecordDiscoveryModule.builder().records(records);
                params.get("protocol").ifPresent(sd::protocol);
                params.getInteger("port").ifPresent(sd::port);
                module.discovery(sd.build());
                break;
            default:
                throw new IllegalArgumentException("illegal value for discovery");
        }

        params.getBoolean("useLocal").ifPresent(module::useLocal);

        // @formatter:off
        return HeroicConfig.builder().cluster(module);
        // @formatter:on
    }

    public URI buildUri(final String node) {
        try {
            return new URI(node);
        } catch (final URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + node, e);
        }
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("cluster");
    }

    @Override
    public String description() {
        return "Configured cluster and discovery method";
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("discovery", "Discovery method to use, valid are: static, srv",
                    "<type>"),
            parameter("host", "Host to add to list of static nodes to discover, can be " +
                    "used multiple times", "<uri>"),
            parameter("record", "Record to add to lookup through SRV, can be used " +
                    "multiple times", "<srv>"),
            parameter("protocol", "Protocol to use for looked up SRV records, default: " +
                    "nativerpc", "<protocol>"),
            parameter("port", "Port to use for looked up SRV records, default: 1394",
                "<port>")
        );
        // @formatter:on
    }
}

/*
 * Copyright (c) 2020 Spotify AB.
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

package com.spotify.heroic.tracing;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.isNull;

import com.google.cloud.MetadataConfig;
import io.opencensus.trace.AttributeValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EnvironmentMetadata {
    /** The absolute max size of attributes, including optional ones */
    private static final int MAX_ATTRIBUTE_SIZE = 8;
    private static Map<String, AttributeValue> attributes;

    private EnvironmentMetadata() { }

    private static void addGcpMetadata() {

        String projectId = MetadataConfig.getProjectId();

        if (isNull(projectId) || projectId.isBlank()) {
            return;
        }

        attributes.put("gcp.project", stringAttributeValue(MetadataConfig.getProjectId()));
        String zone = MetadataConfig.getZone();
        attributes.put("gcp.zone", stringAttributeValue(zone));
        attributes.put(
            "gcp.region", stringAttributeValue(zone.substring(0, zone.lastIndexOf("-"))));
    }

    private static void addHostname() {
        final Optional<String> hostname = EnvVariableLookup.getEnvVariable("HOSTNAME");
        hostname.ifPresent(h -> attributes.put("hostname", stringAttributeValue(h)));
    }

    private static void initialize() {
        attributes = new HashMap<>(MAX_ATTRIBUTE_SIZE);

        attributes.put("java.version", stringAttributeValue(System.getProperty("java.version")));

        addHostname();
        addGcpMetadata();

        EnvironmentMetadata.attributes = unmodifiableMap(attributes);
    }

    @SuppressWarnings("WeakerAccess")
    public static EnvironmentMetadata create() {
        // thread-safe initialization
        if (isNull(EnvironmentMetadata.attributes)) {
            synchronized (EnvironmentMetadata.class) {
                if (isNull(EnvironmentMetadata.attributes)) {
                    EnvironmentMetadata.initialize();
                }
            }
        }
        return new EnvironmentMetadata();
    }

    public Map<String, AttributeValue> toAttributes() {
        return attributes;
    }
}

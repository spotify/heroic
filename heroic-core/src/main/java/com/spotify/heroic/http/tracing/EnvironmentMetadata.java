/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.http.tracing;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;

import com.google.auto.value.AutoValue;
import com.google.cloud.MetadataConfig;
import io.opencensus.trace.AttributeValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@AutoValue
public abstract class EnvironmentMetadata {
    public abstract Optional<String> gcpZone();
    public abstract Optional<String> gcpInstanceId();
    public abstract Optional<String> gcpProject();
    public abstract Optional<String> gcpRegion();
    public abstract Optional<String> role();
    public abstract Optional<String> gcpName();

    public static EnvironmentMetadata create() {
        final String instanceId = MetadataConfig.getInstanceId();
        if (instanceId != null) {
            final String project = MetadataConfig.getProjectId();
            final String zone = MetadataConfig.getZone();
            final String region = zone.substring(0, zone.lastIndexOf("-"));
            final String role = MetadataConfig.getAttribute("instance/attributes/role");
            final String name = MetadataConfig.getAttribute("instance/name");

            return new AutoValue_EnvironmentMetadata(
                Optional.of(zone),
                Optional.of(instanceId),
                Optional.of(project),
                Optional.of(region),
                Optional.ofNullable(role),
                Optional.ofNullable(name)
          );
        } else {
            return new AutoValue_EnvironmentMetadata(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
            );
          }
        }

    public Map<String, AttributeValue> toAttributes() {
        final HashMap<String, AttributeValue> map = new HashMap<>();

        addOptionalAttribute(map, gcpZone(), "gcp.zone");
        addOptionalAttribute(map, gcpInstanceId(), "gcp.instance_id");
        addOptionalAttribute(map, gcpProject(), "gcp.project");
        addOptionalAttribute(map, gcpRegion(), "gcp.region");
        addOptionalAttribute(map, role(), "role");
        addOptionalAttribute(map, gcpName(), "gcp.name");

        return map;
    }

    private void addOptionalAttribute(
        HashMap<String, AttributeValue> attributes, Optional<String> value, String name
    ) {
        value.ifPresent(v -> attributes.put(name, stringAttributeValue(v)));
    }

}

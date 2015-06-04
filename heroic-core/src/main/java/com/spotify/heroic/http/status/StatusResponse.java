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

package com.spotify.heroic.http.status;

import lombok.Data;

@Data
public class StatusResponse {
    @Data
    public static class Consumer {
        private final boolean ok;
        private final int available;
        private final int ready;

        /**
         * Total number of consumer errors encountered.
         */
        private final long errors;
        private final long consumingThreads;
        private final long totalThreads;
    }

    @Data
    public static class Backend {
        private final boolean ok;
        private final int available;
        private final int ready;
    }

    @Data
    public static class MetadataBackend {
        private final boolean ok;
        private final int available;
        private final int ready;
    }

    @Data
    public static class Cluster {
        private final boolean ok;
        private final int onlineNodes;
        private final int offlineNodes;
    }

    private final boolean ok;
    private final Consumer consumers;
    private final Backend backends;
    private final MetadataBackend metadataBackends;
    private final Cluster cluster;
}

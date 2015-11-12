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

import java.util.List;

import lombok.Data;

@Data
public class CollectdSample {
    public static final int COUNTER = 0;
    public static final int GAUGE = 1;
    public static final int DERIVE = 2;
    public static final int ABSOLUTE = 3;

    private final String host;
    private final long time;
    private final String plugin;
    private final String pluginInstance;
    private final String type;
    private final String typeInstance;
    private final List<CollectdValue> values;
    private final long interval;
    private final String message;
    private final long severity;
}

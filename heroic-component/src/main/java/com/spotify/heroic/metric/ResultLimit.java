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

package com.spotify.heroic.metric;

/**
 * Designates a single limit that was violated for a given query.
 */
public enum ResultLimit {
    /**
     * The number of time series required were higher than the specified limit.
     */
    SERIES,

    /**
     * The number of result groups required were larger than the allowed limit.
     */
    GROUP,

    /**
     * The amount of data that needed to be fetched from the backend were larger than the allowed
     * limit.
     */
    QUOTA,

    /**
     * The number of samples retained in Aggregations was higher than the allowed quota.
     */
    AGGREGATION
}

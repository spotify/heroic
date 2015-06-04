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

public interface FetchQuotaWatcher {
    /**
     * Indicates that backend has read {@code n} more datapoints.
     *
     * @param n
     *            The number of datapoints read by the backend.
     * @return A {@code boolean} indicating weither the operation may continue or not.
     */
    public boolean readData(long n);

    /**
     * Indicates if readData quota has been breached or not.
     * 
     * @return {@code true} if there is data left to be read, {@code false} otherwise.
     */
    public boolean mayReadData();

    /**
     * Get how much data you are allowed to read.
     */
    public int getReadDataQuota();

    /**
     * Indicates if any quota has been violated.
     *
     * @return {@code true} if any quota was violated, {@code false} otherwise.
     */
    public boolean isQuotaViolated();
}
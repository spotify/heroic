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

package com.spotify.heroic.statistics;

/*
 * Keep track of amount of expected in-memory data. One instance of this class per operation (i.e.
 * a query).
 */
public interface DataInMemoryReporter {
    /**
     * report that rows in metric backend has been accessed
     *
     * @param n amount of rows
     */
    void reportRowsAccessed(long n);

    /**
     * report the density of a row that was read from the metric backend
     *
     * @param msBetweenSamples density, number of samples per hour
     */
    void reportRowDensity(long msBetweenSamples);

    /**
     * report that data has been read into memory
     *
     * @param n amount of data
     */
    void reportDataHasBeenRead(long n);

    /**
     * report that data is no longer needed in memory
     * <p>
     * The data in question is expected to be garbage collected. If it isn't, then something else is
     * unexpectantly having a reference to it, which could indicate a problem/bug.
     *
     * @param n amount of data
     */
    void reportDataNoLongerNeeded(long n);

    /**
     * report that the whole operation is done, meaning that all data referenced in this
     * DataInMemory instance is expected to be garbage collected.
     */
    void reportOperationEnded();
}

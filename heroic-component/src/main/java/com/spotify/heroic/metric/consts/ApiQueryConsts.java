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

package com.spotify.heroic.metric.consts;

@SuppressWarnings({"LineLength"})
public class ApiQueryConsts {
    /**
     * MutateRpcTimeoutMs
     *
     * The amount of milliseconds to wait before issuing a client side timeout
     * for mutation remote procedure calls.
     *
     * <p>Also defined as (source to be rediscovered...) :
     * If timeouts are set, how many milliseconds should pass before a
     * DEADLINE_EXCEEDED for a long mutation. Currently, this feature is experimental.
     *
     * @see <a href="https://cloud.google.com/bigtable/docs/hbase-client/javadoc/com/google/cloud/bigtable/config/CallOptionsConfig.Builder.html#setmutaterpctimeoutms">CallOptionsConfig.Builder#MutateRpcTimeoutMs</a>
     */
    public static final int DEFAULT_MUTATE_RPC_TIMEOUT_MS = 1_000;

    /**
     * ReadRowsRpcTimeoutMs
     *
     * The amount of milliseconds to wait before issuing a client side
     * timeout for readRows streaming remote procedure calls.
     *
     * <p>AKA
     * The default duration to wait before timing out read stream RPC
     * (default value: 12 hour).
     * @see <a href="https://cloud.google.com/bigtable/docs/hbase-client/javadoc/com/google/cloud/bigtable/config/CallOptionsConfig.Builder.html#setreadrowsrpctimeoutms">ReadRowsRpcTimeoutMs</a>
     */
    public static final int DEFAULT_READ_ROWS_RPC_TIMEOUT_MS = 1_000;

    /**
     * ShortRpcTimeoutMs
     * The amount of milliseconds to wait before issuing a client side timeout for
     * short remote procedure calls.
     * TODO get from Adam proper description
     *
     * <p>AKA
     * The default duration to wait before timing out RPCs (default Google value: 60
     * seconds) @see <a href="https://cloud.google.com/bigtable/docs/hbase-client/javadoc/com/google/cloud/bigtable/config/CallOptionsConfig#SHORT_TIMEOUT_MS_DEFAULT">CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT</a>
     */
    public static final int DEFAULT_SHORT_RPC_TIMEOUT_MS = 1_000;

    /**
     * Maximum number of times to retry after a scan timeout (Google default value: 10 retries).
     * Note that we're going with 3 retries since that's what the common-config BT repo. Note
     * that that repo specifies "max-attempts" so we want 3-1 = 2.
     *  @see <a href=https://cloud.google.com/bigtable/docs/hbase-client/javadoc/com/google/cloud/bigtable/config/RetryOptions.html#getmaxscantimeoutretries">RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES</a>
     */
    public static final int DEFAULT_MAX_SCAN_TIMEOUT_RETRIES  = 2;

    /**
    * Copy of com.google.cloud.bigtable.config.RetryOptions#DEFAULT_INITIAL_BACKOFF_MILLIS
    * so that we don't have to link/depend on the Google jar
    * <p></p>
    * Initial amount of time to wait before retrying failed operations (default value: 5ms).
    **/
    public static final int DEFAULT_INITIAL_BACKOFF_MILLIS = 5;

    /**
    * Copy of com.google.cloud.bigtable.config.RetryOptions#DEFAULT_BACKOFF_MULTIPLIER
    * So that we don't have to link/depend on the Google jar
    * <p></p>
    * Multiplier to apply to wait times after failed retries (default value: 1.5).
    * */
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;

    /**
     * A little "safety buffer" to err on the side of caution (against ceasing
     * retrying prematurely).
     */
    private static final int SAFETY_BUFFER_MILLIS = 25;

    /**
     * Maximum amount of time to retry before failing the operation (Google default value: 600
     * seconds).
     * <p></p>
     * From Adam Steele [adamsteele@google.com]:
     * The operation will be retried until you hit either maxElapsedBackoffMs or (for scan
     * operations) maxScanTimeoutRetries.
     * <p></p>
     * So, we use com.google.cloud.bigtable.config.RetryOptions#DEFAULT_BACKOFF_MULTIPLIER
     * and com.google.cloud.bigtable.config.RetryOptions#DEFAULT_INITIAL_BACKOFF_MILLIS
     * to come up with a number of millis, which is currently :
     * <p></p>
     * 3 * 1000 * 1.5 + 5 + 25 = 4,530 ms total potential wait for an operation
     * <p></p>
     * which is pretty reasonable.
     */
    public static final int DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS = (int)
            ((1 + DEFAULT_MAX_SCAN_TIMEOUT_RETRIES) *
                    DEFAULT_READ_ROWS_RPC_TIMEOUT_MS *
                    DEFAULT_BACKOFF_MULTIPLIER) +
            DEFAULT_INITIAL_BACKOFF_MILLIS +
            SAFETY_BUFFER_MILLIS;
}

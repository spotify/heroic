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

package com.spotify.heroic.metric.datastax;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * An aggressive retrying policy that will force the driver to retry a certain request a given
 * number of times before giving up.
 *
 * Every {@code rotateHost} request will cause the decision to be to
 * {@link RetryDecision#tryNextHost}.
 *
 * This policy is <em>not</em> suitable for production use. It is intended for troubleshooting
 * purposes only.
 *
 * @author udoprog
 */
@Data
@Slf4j
public class AggressiveRetryPolicy implements RetryPolicy {
    private final int numRetries;
    private final int rotateHost;

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl,
            int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry < numRetries) {
            final String stmt;

            if (statement instanceof BoundStatement) {
                final BoundStatement bound = (BoundStatement) statement;
                final List<Object> variables = new ArrayList<>();

                for (int i = 0; i < bound.preparedStatement().getVariables().size(); i++) {
                    variables.add(bound.getObject(i));
                }

                stmt = String.format("%s (%s)", bound.preparedStatement().getQueryString(),
                        variables.toString());
            } else {
                stmt = statement.toString();
            }

            final boolean tryNextHost = nbRetry % rotateHost == (rotateHost - 1);

            log.info("Request failing ({}/{}) retrying ({}) (decision: {})...", nbRetry, numRetries,
                    stmt, tryNextHost ? "tryNextHost" : "retry");

            return tryNextHost ? RetryDecision.tryNextHost(cl) : RetryDecision.retry(cl);
        }

        return receivedResponses >= requiredResponses && !dataRetrieved ? RetryDecision.retry(cl)
                : RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl,
            WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0) {
            return RetryDecision.rethrow();
        }

        // If the batch log write failed, retry the operation as this might just be we were
        // unlucky at picking candidates
        return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl,
            int requiredReplica, int aliveReplica, int nbRetry) {
        return (nbRetry == 0) ? RetryDecision.tryNextHost(cl) : RetryDecision.rethrow();
    }
}

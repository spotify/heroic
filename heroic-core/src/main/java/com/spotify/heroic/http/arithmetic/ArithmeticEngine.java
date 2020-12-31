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

package com.spotify.heroic.http.arithmetic;

import com.spotify.heroic.metric.Arithmetic;
import com.spotify.heroic.metric.QueryMetricsResponse;
import java.util.Map;

public interface ArithmeticEngine {
    /**
     * Apply the supplied operation to the supplied query responses.
     * <p>
     * NOTE that: 1. the operation must reference the names (keys) of the responses in
     * `queryResponses`. So, for example, if `operation` is 'A / B', then `queryResponses`' keys
     * must be 'A' and 'B'. 2. Every series within the supplied responses must be :
     * <p>- the same length
     * <p>- contain the exact same timestamps
     * <p>- be > 1 in length
     * @param operation      e.g. 'A / B * 100' to get a ratio of A's to B's.
     * @param queryResponses e.g.
     * <p>{
     * <p>     "A" → { ... , [1.4,2.8,...]},
     * <p>     "B" → { ..., [0.8,0.1,...]}
     * <p>}
     * <p> where "A" and "B" are responses to queries named as such in Grafana.
     *
     * @return the resulting responses once the `operation` has been applied to the
     * queryResponses.
     */
    QueryMetricsResponse run(Arithmetic arithmetic,
        Map<String, QueryMetricsResponse> queryResponses);
}

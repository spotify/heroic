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

package com.spotify.heroic.common;

public class Throwing {
    /**
     * Guarantee that both clauses are called, in the given order. Combine any thrown exceptions by
     * adding any prior exceptions as suppressed.
     *
     * This is useful if you want to have multiple potentially throwing blocks of code, and
     * guarantee that all have been called at least once.
     *
     * @param a First block to call.
     * @param b Second block to call.
     * @throws Exception If any of the blocks throw an exception.
     */
    public static void call(final CanThrow a, final CanThrow b) throws Exception {
        Exception inner = null;

        try {
            a.call();
        } catch (final Exception e) {
            inner = e;
        }

        try {
            b.call();
        } catch (final Exception e) {
            if (inner != null) {
                e.addSuppressed(inner);
            }

            throw e;
        }

        if (inner != null) {
            throw inner;
        }
    }

    public static interface CanThrow {
        public void call() throws Exception;
    }
}

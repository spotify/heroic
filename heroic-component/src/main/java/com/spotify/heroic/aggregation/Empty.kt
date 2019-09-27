/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.aggregation

object Empty: Aggregation {
    const val NAME = "empty"

    override fun apply(p0: AggregationContext?): EmptyInstance = EmptyInstance.INSTANCE

    // Jackson currently creates multiple instances of singleton objects, causing equality
    // to fail. Overriding equals is necessary until this is fixed:
    // https://github.com/FasterXML/jackson-module-kotlin/issues/225
    override fun equals(other: Any?): Boolean {
        return other is Empty
    }
}
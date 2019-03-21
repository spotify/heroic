/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.requestcondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.heroic.querylogging.QueryContext;
import java.util.List;

/**
 * A request condition that match if all child conditions match.
 */
@AutoValue
public abstract class All implements RequestCondition {
    @JsonCreator
    public static All create(@JsonProperty("conditions") List<RequestCondition> conditions) {
        return new AutoValue_All(conditions);
    }

    abstract List<RequestCondition> conditions();

    @Override
    public boolean matches(QueryContext context) {
        return conditions().stream().allMatch(c -> c.matches(context));
    }
}

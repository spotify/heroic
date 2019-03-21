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

package com.spotify.heroic.conditionalfeatures;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.querylogging.QueryContext;

/**
 * Matches all features listed, one by one.
 */
@AutoValue
public abstract class List implements ConditionalFeatures {
    @JsonCreator
    public static List create(@JsonProperty("list") java.util.List<ConditionalFeatures> list) {
        return new AutoValue_List(list);
    }

    public abstract java.util.List<ConditionalFeatures> list();

    @Override
    public FeatureSet match(QueryContext queryContext) {
        FeatureSet features = FeatureSet.empty();

        for (final ConditionalFeatures conditionalFeatures : list()) {
            features = features.combine(conditionalFeatures.match(queryContext));
        }

        return features;
    }
}

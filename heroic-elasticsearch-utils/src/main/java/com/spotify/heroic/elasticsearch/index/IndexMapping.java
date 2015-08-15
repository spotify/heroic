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

package com.spotify.heroic.elasticsearch.index;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.common.DateRange;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = RotatingIndexMapping.class, name = "rotating"),
        @JsonSubTypes.Type(value = SingleIndexMapping.class, name = "single") })
public interface IndexMapping {
    public String template();

    public String[] readIndices(DateRange range) throws NoIndexSelectedException;

    public String[] writeIndices(DateRange range) throws NoIndexSelectedException;

    public SearchRequestBuilder search(Client client, DateRange range, String type) throws NoIndexSelectedException;

    public DeleteByQueryRequestBuilder deleteByQuery(Client client, DateRange range, String type)
            throws NoIndexSelectedException;

    public CountRequestBuilder count(Client client, DateRange range, String metadataType)
            throws NoIndexSelectedException;
}

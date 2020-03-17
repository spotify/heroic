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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = RotatingIndexMapping.class, name = "rotating"),
    @JsonSubTypes.Type(value = SingleIndexMapping.class, name = "single")
})
public interface IndexMapping {
    // Index template
    String template();

    Map<String, Object> settings();

    String[] readIndices(String type) throws NoIndexSelectedException;

    String[] writeIndices(String type) throws NoIndexSelectedException;

    SearchRequestBuilder search(Client client, String type) throws NoIndexSelectedException;

    SearchRequestBuilder count(Client client, String type) throws NoIndexSelectedException;

    /**
     * Create a delete request using the given client.
     *
     * @param client Client to create request with
     * @param type Type of document to delete
     * @param id Id of document to delete
     * @return a new delete request
     */
    List<DeleteRequestBuilder> delete(Client client, String type, String id)
        throws NoIndexSelectedException;
}

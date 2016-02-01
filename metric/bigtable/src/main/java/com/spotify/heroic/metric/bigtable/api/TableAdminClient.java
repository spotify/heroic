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

package com.spotify.heroic.metric.bigtable.api;

import java.util.Optional;

public interface TableAdminClient {
    /**
     * Get details about a table if it exists.
     *
     * @param tableId The id of the table to get details about.
     *
     * @return The table, or empty if it does not exist.
     */
    public Optional<Table> getTable(String tableId);

    /**
     * Create the specified table.
     *
     * @return The created table.
     */
    public Table createTable(String name);

    /**
     * Create the specified column family.
     *
     * @return The created column family.
     */
    public ColumnFamily createColumnFamily(Table table, String name);
}

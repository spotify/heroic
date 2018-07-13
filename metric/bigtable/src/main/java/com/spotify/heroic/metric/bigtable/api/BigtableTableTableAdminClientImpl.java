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

import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.spotify.heroic.bigtable.io.grpc.Status;
import com.spotify.heroic.bigtable.io.grpc.StatusRuntimeException;
import eu.toolchain.async.AsyncFramework;
import lombok.ToString;

import java.util.Optional;

@ToString
public class BigtableTableTableAdminClientImpl implements BigtableTableAdminClient {
    final com.google.cloud.bigtable.grpc.BigtableTableAdminClient client;

    private final String project;
    private final String instance;

    private final String clusterUri;

    private final BigtableInstanceName bigtableInstanceName;

    public BigtableTableTableAdminClientImpl(
        final AsyncFramework async,
        final com.google.cloud.bigtable.grpc.BigtableTableAdminClient client, final String project,
        final String instance
    ) {
        this.client = client;
        this.project = project;
        this.instance = instance;
        this.clusterUri = String.format("projects/%s/instances/%s", project, instance);
        this.bigtableInstanceName = new BigtableInstanceName(project, instance);
    }

    @Override
    public Optional<Table> getTable(String name) {
        try {
            return Optional.of(Table.fromPb(client.getTable(
                com.google.bigtable.admin.v2.GetTableRequest
                    .newBuilder()
                    .setName(Table.toURI(clusterUri, name))
                    .build())));
        } catch (final StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                return Optional.empty();
            }

            throw e;
        }
    }

    @Override
    public Table createTable(String name) {
        client.createTable(com.google.bigtable.admin.v2.CreateTableRequest
            .newBuilder()
            .setParent(bigtableInstanceName.toString())
            .setTableId(name)
            .build());

        return new Table(clusterUri, name);
    }

    @Override
    public ColumnFamily createColumnFamily(Table table, String name) {
        final ModifyColumnFamiliesRequest.Modification modification =
          ModifyColumnFamiliesRequest.Modification
            .newBuilder()
            .setCreate(com.google.bigtable.admin.v2.ColumnFamily.newBuilder().build())
            .setId(name).build();

        client.modifyColumnFamily(com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest
            .newBuilder()
            .addModifications(modification)
            .setName(bigtableInstanceName.toTableNameStr(table.getName()))
            .build());

        return new ColumnFamily(clusterUri, table.getName(), name);
    }
}

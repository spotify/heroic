package com.spotify.heroic.metric.bigtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.bigtable.admin.table.v1.ListTablesResponse;
import com.google.bigtable.admin.table.v1.Table;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.ColumnRange;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.spotify.heroic.metric.bigtable.api.BigtableCell;
import com.spotify.heroic.metric.bigtable.api.BigtableClient;
import com.spotify.heroic.metric.bigtable.api.BigtableColumnFamily;
import com.spotify.heroic.metric.bigtable.api.BigtableColumnFamilyBuilder;
import com.spotify.heroic.metric.bigtable.api.BigtableLatestColumnFamily;
import com.spotify.heroic.metric.bigtable.api.BigtableLatestRow;
import com.spotify.heroic.metric.bigtable.api.BigtableMutations;
import com.spotify.heroic.metric.bigtable.api.BigtableMutationsBuilder;
import com.spotify.heroic.metric.bigtable.api.BigtableRowFilter;
import com.spotify.heroic.metric.bigtable.api.BigtableTable;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableBuilder;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private final String USER_AGENT = "heroic";

    private final String project;
    private final String zone;
    private final String cluster;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;
    private final ExecutorService executorService;

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final BigtableOptions options = new BigtableOptions.Builder().setProjectId(project).setZoneId(zone)
                .setClusterId(cluster).setUserAgent(USER_AGENT)
                .setDataChannelCount(64)
                .setCredentialOptions(credentials).build();

        final BigtableSession session = new BigtableSession(options, executorService);

        final BigtableTableAdminClient adminClient = new BigtableAdminClientImpl(session);
        final BigtableClient client = new BigtableClientImpl(session);

        return new GrpcBigtableConnection(project, zone, cluster, session, adminClient, client);
    }

    @RequiredArgsConstructor
    @ToString(of = { "project", "zone", "cluster" })
    public static class GrpcBigtableConnection implements BigtableConnection {
        private final String project;
        private final String zone;
        private final String cluster;

        final BigtableSession session;
        final BigtableTableAdminClient adminClient;
        final BigtableClient client;

        @Override
        public BigtableTableAdminClient adminClient() {
            return adminClient;
        }

        @Override
        public BigtableClient client() {
            return client;
        }

        @Override
        public void close() throws Exception {
            session.close();
        }
    }

    @RequiredArgsConstructor
    @ToString
    class BigtableAdminClientImpl implements BigtableTableAdminClient {
        final BigtableSession session;
        final String clusterUri = String.format("projects/%s/zones/%s/clusters/%s", project, zone, cluster);

        @Override
        public BigtableTable getTable(String name) throws IOException {
            final GetTableRequest request = GetTableRequest.newBuilder().setName(name).build();
            final Table tableDetails = session.getTableAdminClient().getTable(request);

            final String tableName = tableUriToName(tableDetails.getName());

            final List<BigtableColumnFamily> columnFamilies = new ArrayList<>();

            for (final Entry<String, ColumnFamily> e : tableDetails.getColumnFamilies().entrySet()) {
                final ColumnFamily v = e.getValue();
                columnFamilies.add(new BigtableColumnFamily(columnFamilyUriToName(tableName, v.getName())));
            }

            return new BigtableTable(tableName, ImmutableList.copyOf(columnFamilies));
        }

        @Override
        public void createTable(BigtableTable table) throws IOException {
            final CreateTableRequest request = CreateTableRequest.newBuilder().setName(clusterUri)
                    .setTableId(table.getName())
                    .build();

            session.getTableAdminClient().createTable(request);

            for (final BigtableColumnFamily family : table.getColumnFamilies()) {
                createColumnFamily(table.getName(), family);
            }
        }

        @Override
        public void createColumnFamily(String table, BigtableColumnFamily family) throws IOException {
            // name MUST be empty during creation, do not set it.
            final ColumnFamily cf = ColumnFamily.newBuilder().build();

            final CreateColumnFamilyRequest request = CreateColumnFamilyRequest.newBuilder().setName(tableNameToUri(table))
                    .setColumnFamilyId(family.getName()).setColumnFamily(cf)
                    .build();

            session.getTableAdminClient().createColumnFamily(request);
        }

        @Override
        public List<BigtableTable> listTablesDetails() throws IOException {
            final ListTablesRequest request = ListTablesRequest.newBuilder().setName(clusterUri).build();

            final ListTablesResponse response = session.getTableAdminClient().listTables(request);

            final List<BigtableTable> tables = new ArrayList<>();

            for (final Table table : response.getTablesList()) {
                tables.add(getTable(table.getName()));
            }

            return tables;
        }

        @Override
        public BigtableTableBuilder table(String name) {
            return new BigtableTableBuilder(name);
        }

        @Override
        public BigtableColumnFamilyBuilder columnFamily(String name) {
            return new BigtableColumnFamilyBuilder(name);
        };

        String columnFamilyUriToName(String tableName, String name) {
            final String tableUri = tableNameToUri(tableName);

            if (!name.startsWith(tableUri))
                throw new IllegalArgumentException(String.format(
                        "Somehow you are converting a table (%s) from a different cluster (%s)", name, tableUri));

            return name.substring(tableUri.length() + "/columnFamilies/".length(), name.length());
        }

        String tableUriToName(String name) {
            if (!name.startsWith(clusterUri))
                throw new IllegalArgumentException(String.format(
                        "Somehow you are converting a table (%s) from a different cluster (%s)", name, clusterUri));

            return name.substring(clusterUri.length() + "/tables/".length(), name.length());
        }

        String tableNameToUri(String table) {
            return String.format("%s/tables/%s", clusterUri, table);
        }

        String columnFamilyNameToUri(String table, String columnFamily) {
            return String.format("%s/tables/%s/columnFamilies/%s", clusterUri, table, columnFamily);
        }
    }

    @RequiredArgsConstructor
    @ToString
    class BigtableClientImpl implements BigtableClient {
        final BigtableSession session;

        final String clusterUri = String.format("projects/%s/zones/%s/clusters/%s", project, zone, cluster);

        @Override
        public AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, BigtableMutations mutations) {
            final ListenableFuture<Empty> request;

            try {
                request = session.getDataClient().mutateRowAsync(
                        MutateRowRequest.newBuilder().setTableName(tableName(tableName)).setRowKey(rowKey)
                                .addAllMutations(mutations.getMutations()).build());
            } catch (final Exception e) {
                return async.failed(e);
            }

            return convertEmpty(request);
        }

        String columnFamilyUriToName(String tableName, String name) {
            final String tableUri = tableNameToUri(tableName);

            if (!name.startsWith(tableUri))
                throw new IllegalArgumentException(String.format(
                        "Somehow you are converting a table (%s) from a different cluster (%s)", name, tableUri));

            return name.substring(tableUri.length() + "/columnFamilies/".length(), name.length());
        }

        String tableNameToUri(String table) {
            return String.format("%s/tables/%s", clusterUri, table);
        }

        @Override
        public BigtableMutationsBuilder mutations() {
            return new BigtableMutationsBuilder();
        }

        String tableName(String tableName) {
            return String.format("%s/tables/%s", clusterUri, tableName);
        }

        @Override
        public AsyncFuture<List<BigtableLatestRow>> readRows(final String tableName, ByteString rowKey,
                BigtableRowFilter filter) {
            final ReadRowsRequest request = ReadRowsRequest.newBuilder().setTableName(tableName(tableName))
                    .setRowKey(rowKey).setFilter(filter.getFilter()).build();

            final ListenableFuture<List<Row>> readRowsAsync;

            try {
                readRowsAsync = session.getDataClient().readRowsAsync(request);
            } catch (final IOException e) {
                return async.failed(e);
            }

            return convert(readRowsAsync).directTransform(result -> {
                final List<BigtableLatestRow> rows = new ArrayList<>();

                for (final Row row : result) {
                    final List<BigtableLatestColumnFamily> families = new ArrayList<>();

                    for (final Family family : row.getFamiliesList()) {
                        final Iterable<BigtableCell> columns = makeColumnIterator(family);
                        families.add(new BigtableLatestColumnFamily(family.getName(), columns));
                    }

                    rows.add(new BigtableLatestRow(families));
                }

                return rows;
            });
        }

        @Override
        public BigtableRowFilter columnFilter(String family, ByteString start, ByteString end) {
            final ColumnRange range = ColumnRange.newBuilder().setFamilyName(family).setStartQualifierExclusive(start)
                    .setEndQualifierInclusive(end).build();

            return new BigtableRowFilter(RowFilter.newBuilder().setColumnRangeFilter(range).build());
        }
    }

    <T> AsyncFuture<T> convert(final ListenableFuture<T> request) {
        final ResolvableFuture<T> future = async.future();

        Futures.addCallback(request, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }

    AsyncFuture<Void> convertEmpty(final ListenableFuture<Empty> request) {
        final ResolvableFuture<Void> future = async.future();

        Futures.addCallback(request, new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
                future.resolve(null);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }

    static Iterable<BigtableCell> makeColumnIterator(final Family family) {
        return new Iterable<BigtableCell>() {

            @Override
            public Iterator<BigtableCell> iterator() {
                final Iterator<Column> iterator = family.getColumnsList().iterator();

                return new Iterator<BigtableCell>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public BigtableCell next() {
                        final Column next = iterator.next();

                        final ByteString qualifier = next.getQualifier();
                        final ByteString value = next.getCells(0).getValue();

                        return new BigtableCell(qualifier, value);
                    }

                    @Override
                    public void remove() {
                        throw new IllegalStateException();
                    }
                };
            }
        };
    }
}
package com.spotify.heroic.metric.datastax.schema.ng;

import java.io.IOException;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.datastax.Async;
import com.spotify.heroic.metric.datastax.schema.AbstractCassandraSchema;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NextGenSchema extends AbstractCassandraSchema implements Schema {
    public static final String CREATE_KEYSPACE_CQL = NextGenSchema.class.getPackage().getName()
            + "/keyspace.cql";
    public static final String CREATE_TABLES_CQL = NextGenSchema.class.getPackage().getName() + "/tables.cql";

    private static final String WRITE_METRICS_CQL = "INSERT INTO {{keyspace}}.metrics (metric_key, data_timestamp_offset, data_value) VALUES (?, ?, ?)";
    private static final String FETCH_METRICS_CQL = "SELECT data_timestamp_offset, data_value FROM {{keyspace}}.metrics WHERE metric_key = ? and data_timestamp_offset >= ? and data_timestamp_offset <= ? LIMIT ?";
    private static final String DELETE_METRICS_CQL = "DELETE FROM {{keyspace}}.metrics WHERE metric_key = ?";
    private static final String KEYS_PAGING_LIMIT = "SELECT DISTINCT metric_key FROM {{keyspace}}.metrics limit ?";
    private static final String KEYS_PAGING_LEFT_LIMIT = "SELECT DISTINCT metric_key FROM {{keyspace}}.metrics WHERE token(metric_key) > token(?) limit ?";

    private final String keyspace;

    public NextGenSchema(final AsyncFramework async, final String keyspace) {
        super(async);
        this.keyspace = keyspace;
    }

    @Override
    public AsyncFuture<Void> configure(final Session s) {
        final Map<String, String> values = ImmutableMap.of("keyspace", keyspace);

        final AsyncFuture<PreparedStatement> createKeyspace;

        try {
            createKeyspace = prepareTemplate(values, s, CREATE_KEYSPACE_CQL);
        } catch (IOException e) {
            return async.failed(e);
        }

        return createKeyspace.lazyTransform(createKeyspaceStmt -> {
            log.info("Creating keyspace {}", keyspace);

            return Async.bind(async, s.executeAsync(createKeyspaceStmt.bind())).lazyTransform(ign -> {
                final AsyncFuture<PreparedStatement> createTables = prepareTemplate(values, s, CREATE_TABLES_CQL);

                return createTables.lazyTransform(createTablesStmt -> {
                    log.info("Creating tables for keyspace {}", keyspace);
                    return Async.bind(async, s.executeAsync(createTablesStmt.bind())).directTransform(ign2 -> null);
                });
            });
        });
    }

    @Override
    public AsyncFuture<SchemaInstance> instance(Session s) {
        final Map<String, String> values = ImmutableMap.of("keyspace", keyspace);

        final AsyncFuture<PreparedStatement> write = prepareAsync(values, s, WRITE_METRICS_CQL);
        final AsyncFuture<PreparedStatement> fetch = prepareAsync(values, s, FETCH_METRICS_CQL);
        final AsyncFuture<PreparedStatement> delete = prepareAsync(values, s, DELETE_METRICS_CQL);
        final AsyncFuture<PreparedStatement> keysPagingLimit = prepareAsync(values, s, KEYS_PAGING_LIMIT);
        final AsyncFuture<PreparedStatement> keysPagingLeftLimit = prepareAsync(values, s, KEYS_PAGING_LEFT_LIMIT);

        return async.collectAndDiscard(ImmutableList.of(write, fetch, delete, keysPagingLimit, keysPagingLeftLimit))
                .directTransform(r -> {
                    return new NextGenSchemaInstance(write.getNow(), fetch.getNow(), delete.getNow(),
                            keysPagingLimit.getNow(), keysPagingLeftLimit.getNow());
                });
    }
}
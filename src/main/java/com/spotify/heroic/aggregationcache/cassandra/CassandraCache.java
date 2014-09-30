package com.spotify.heroic.aggregationcache.cassandra;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import lombok.Synchronized;
import lombok.ToString;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.spotify.heroic.aggregationcache.AggregationCacheBackend;
import com.spotify.heroic.aggregationcache.CacheOperationException;
import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;

@ToString(exclude = { "context" })
public class CassandraCache implements AggregationCacheBackend {
    public static final int WIDTH = 1200;

    @Inject
    private AstyanaxContext<Keyspace> context;

    @Inject
    private ReadWriteThreadPools pool;

    private AtomicReference<Keyspace> keyspace = new AtomicReference<>();

    private final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    @Inject
    private AggregationCacheBackendReporter reporter;

    @Override
    public Callback<CacheBackendGetResult> get(final CacheBackendKey key,
            DateRange range) throws CacheOperationException {
        final Keyspace keyspace = this.keyspace.get();

        if (keyspace == null) {
            throw new IllegalStateException("keyspace not available");
        }

        return ConcurrentCallback.newResolve(pool.read(), new CacheGetResolver(
                keyspace, CQL3_CF, key, range));
    }

    @Override
    public Callback<CacheBackendPutResult> put(final CacheBackendKey key,
            final List<DataPoint> datapoints) throws CacheOperationException {
        final Keyspace keyspace = this.keyspace.get();

        if (keyspace == null) {
            throw new IllegalStateException("keyspace not available");
        }

        return ConcurrentCallback.newResolve(pool.write(),
                new CachePutResolver(keyspace, CQL3_CF, key, datapoints));
    }

    @Override
    @Synchronized
    public void start() throws Exception {
        if (this.keyspace.get() != null) {
            return;
        }

        this.context.start();
        this.keyspace.set(this.context.getClient());
    }

    @Override
    @Synchronized
    public void stop() throws Exception {
        if (this.keyspace.get() == null) {
            return;
        }

        this.keyspace.set(null);
        this.context.shutdown();
    }

    @Override
    public boolean isReady() {
        return this.keyspace.get() != null;
    }
}

package com.spotify.heroic.metrics.cassandra;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.model.TimeSerie;

/**
 * A partial and generic backend implementation for cassandra-based metric
 * backends.
 *
 * The keyspace should be accessed using the
 * {@link CassandraMetricBackend#keyspace} method.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public abstract class CassandraMetricBackend implements MetricBackend {
    private final String keyspaceName;
    private final String seeds;
    private final int maxConnectionsPerHost;
    private final Map<String, String> backendTags;

    private AstyanaxContext<Keyspace> context;
    // could be volatile, but this asserts that a request has to fetch-and-store
    // the keyspace _once_ in the context for which it is valid.
    private final AtomicReference<Keyspace> keyspace = new AtomicReference<Keyspace>();

    @Override
    public TimeSerie getPartition() {
        return new TimeSerie(null, backendTags);
    }

    /**
     * Does a partition key for a backend match the specified one.
     *
     * This is used to determine where to route queries and writes.
     *
     * @param match The partition to match.
     * @param partition Partition key of the backend.
     * @return <code>true</code> if there is a match, <code>false</code> otherwise.
     */
    @Override
    public boolean matchesPartition(TimeSerie match) {
        final Map<String, String> tags = match.getTags();

        if ((tags == null || tags.isEmpty()) && !backendTags.isEmpty())
            return false;

        for (Map.Entry<String, String> entry : backendTags.entrySet()) {
            final String value = tags.get(entry.getKey());

            if (value == null) {
                if (entry.getValue() == null)
                    continue;

                return false;
            }

            if (!value.equals(entry.getValue()))
                return false;
        }

        return true;
    }

    @Override
    public boolean isReady() {
        return keyspace() != null;
    }

    protected Keyspace keyspace() {
        return keyspace.get();
    }

    @Override
    public void start() throws Exception {
        log.info("Starting: {}", this);

        final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setCqlVersion("3.0.0").setTargetCassandraVersion("2.0");

        context = new AstyanaxContext.Builder()
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "HeroicConnectionPool").setPort(9160)
                                .setMaxConnsPerHost(maxConnectionsPerHost)
                                .setSeeds(seeds)).forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(config)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace.set(context.getClient());
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping: {}", this);

        context.shutdown();
        keyspace.set(null);
    }
}

package com.spotify.heroic.metadata.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendConfig;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;

@RequiredArgsConstructor
public final class ElasticSearchConfig implements MetadataBackendConfig {
    public static String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static String DEFAULT_INDEX = "heroic";
    public static String DEFAULT_TYPE = "metadata";
    public static boolean DEFAULT_NODE_CLIENT = false;
    public static int DEFAULT_WRITE_BULK_ACTIONS = 1000;
    public static int DEFAULT_CONCURRENT_BULK_REQUESTS = 5;
    public static long DEFAULT_DUMP_INTERVAL = 1;

    private final String id;
    private final ReadWriteThreadPools.Config pools;
    private final List<InetSocketTransportAddress> seeds;
    private final String clusterName;
    private final String index;
    private final String type;
    private final boolean nodeClient;
    private final int bulkActions;
    private final int concurrentBulkRequests;
    private final Long dumpInterval;
    private final XContentBuilder mapping;

    @JsonCreator
    public static ElasticSearchConfig create(
            @JsonProperty("id") String id,
            @JsonProperty("seeds") List<String> seeds,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("index") String index,
            @JsonProperty("type") String type,
            @JsonProperty("nodeClient") Boolean nodeClient,

            @JsonProperty("pools") ReadWriteThreadPools.Config pools,

            @JsonProperty("writeBulkActions") Integer writeBulkActions,
            @JsonProperty("dumpInterval") Long dumpInterval,
            @JsonProperty("concurrentBulkRequests") Integer concurrentBulkRequests) {
        if (clusterName == null)
            clusterName = DEFAULT_CLUSTER_NAME;

        if (index == null)
            index = DEFAULT_INDEX;

        if (type == null)
            type = DEFAULT_TYPE;

        if (nodeClient == null)
            nodeClient = DEFAULT_NODE_CLIENT;

        if (writeBulkActions == null)
            writeBulkActions = DEFAULT_WRITE_BULK_ACTIONS;

        if (concurrentBulkRequests == null)
            concurrentBulkRequests = DEFAULT_CONCURRENT_BULK_REQUESTS;

        if (dumpInterval == null)
            dumpInterval = DEFAULT_DUMP_INTERVAL;

        if (pools == null)
            pools = ReadWriteThreadPools.Config.createDefault();

        XContentBuilder mapping;

        try {
            mapping = buildMapping(type);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create mapping", e);
        }

        final List<InetSocketTransportAddress> s = buildSeeds(seeds);

        return new ElasticSearchConfig(id, pools, s, clusterName, index, type,
                nodeClient, writeBulkActions, concurrentBulkRequests,
                dumpInterval, mapping);
    }

    private static List<InetSocketTransportAddress> buildSeeds(
            final List<String> rawSeeds) {
        final List<InetSocketTransportAddress> seeds = new ArrayList<>();

        for (final String seed : rawSeeds) {
            seeds.add(parseInetSocketTransportAddress(seed));
        }

        return seeds;
    }

    private static InetSocketTransportAddress parseInetSocketTransportAddress(
            final String seed) {
        if (seed.contains(":")) {
            final String parts[] = seed.split(":");
            return new InetSocketTransportAddress(parts[0],
                    Integer.valueOf(parts[1]));
        }

        return new InetSocketTransportAddress(seed, 9300);
    }

    private static XContentBuilder buildMapping(String type) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();

        XContentBuilder b = builder.startObject();

        b = b.startObject(type);
        b = b.startObject("properties");

        b = b.startObject("key").field("type", "string")
                .field("index", "not_analyzed").endObject();

        b = b.startObject("tags").field("type", "nested");

        {
            b = b.startObject("properties");

            {
                b = b.startObject("value").field("type", "string")
                        .field("index", "not_analyzed").endObject();
                b = b.startObject("key").field("type", "string")
                        .field("index", "not_analyzed").endObject();
            }

            b = b.endObject();
        }

        b = b.endObject();

        b = b.endObject();
        b = b.endObject();
        return b;
    }

    @Override
    public Module module(final Key<MetadataBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public MetadataBackendReporter reporter(
                    MetadataBackendManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            @Named("seeds")
            public List<InetSocketTransportAddress> seeds() {
                return seeds;
            }

            @Provides
            @Singleton
            @Named("clusterName")
            public String clusterName() {
                return clusterName;
            }

            @Provides
            @Singleton
            @Named("index")
            public String index() {
                return index;
            }

            @Provides
            @Singleton
            @Named("type")
            public String type() {
                return type;
            }

            @Provides
            @Singleton
            @Named("nodeClient")
            public boolean nodeClient() {
                return nodeClient;
            }

            @Provides
            @Singleton
            @Named("bulkActions")
            public int bulkActions() {
                return bulkActions;
            }

            @Provides
            @Named("concurrentBulkRequests")
            public int concurrentBulkRequests() {
                return concurrentBulkRequests;
            }

            @Provides
            @Singleton
            @Named("dumpInterval")
            public Long dumpInterval() {
                return dumpInterval;
            }

            @Provides
            @Singleton
            @Named("mapping")
            public XContentBuilder mapping() {
                return mapping;
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(MetadataBackendReporter reporter) {
                return pools.construct(reporter.newThreadPool());
            }

            @Override
            protected void configure() {
                bind(key).to(ElasticSearchMetadataBackend.class);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("elasticsearch#%d", i);
    }
}

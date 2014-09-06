package com.spotify.heroic.metadata.elasticsearch;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataOperationException;
import com.spotify.heroic.metadata.MetadataUtils;
import com.spotify.heroic.metadata.elasticsearch.async.DeleteTimeSeriesResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindKeysResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindSeriesResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindTagKeysResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindTagsTransformer;
import com.spotify.heroic.metadata.elasticsearch.model.FindTagKeys;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.MetadataBackendReporter;

@Slf4j
@RequiredArgsConstructor
@ToString(of = { "seeds", "clusterName", "index", "type", "nodeClient",
        "bulkActions", "dumpInterval", "concurrentBulkRequests" })
public class ElasticSearchMetadataBackend implements MetadataBackend {
    public static String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static String DEFAULT_INDEX = "heroic";
    public static String DEFAULT_TYPE = "metadata";
    public static boolean DEFAULT_NODE_CLIENT = false;
    public static int DEFAULT_WRITE_BULK_ACTIONS = 1000;
    public static int DEFAULT_CONCURRENT_BULK_REQUESTS = 5;
    public static long DEFAULT_WRITE_BULK_FLUSH_INTERVAL = 1;

    @JsonCreator
    public static ElasticSearchMetadataBackend create(
            @JsonProperty("seeds") List<String> seeds,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("index") String index,
            @JsonProperty("type") String type,
            @JsonProperty("nodeClient") Boolean nodeClient,

            @JsonProperty("pools") ReadWriteThreadPools pools,

            @JsonProperty("writeBulkActions") Integer writeBulkActions,
            @JsonProperty("writeBulkFlushInterval") Long writeBulkFlushInterval,
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

        if (writeBulkFlushInterval == null)
            writeBulkFlushInterval = DEFAULT_WRITE_BULK_FLUSH_INTERVAL;

        if (pools == null)
            pools = ReadWriteThreadPools.config().build();

        XContentBuilder mapping;

        try {
            mapping = buildMapping(type);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create mapping", e);
        }

        final List<InetSocketTransportAddress> s = buildSeeds(seeds);

        return new ElasticSearchMetadataBackend(pools, s, mapping, clusterName,
                index, type, nodeClient, writeBulkActions,
                writeBulkFlushInterval, concurrentBulkRequests);
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

    /**
     * Common connection abstraction between Node and TransportClient.
     */
    private interface Connection {
        Client client();

        void close() throws Exception;
    }

    private final ReadWriteThreadPools pools;
    private final List<InetSocketTransportAddress> seeds;
    private final XContentBuilder mapping;
    private final String clusterName;
    private final String index;
    private final String type;
    private final boolean nodeClient;
    private final int bulkActions;
    private final Long dumpInterval;
    private final int concurrentBulkRequests;

    @Inject
    private MetadataBackendReporter reporter;

    private final AtomicReference<Connection> connection = new AtomicReference<Connection>();
    private final AtomicReference<BulkProcessor> bulkProcessor = new AtomicReference<BulkProcessor>();

    public Client client() {
        final Connection connection = this.connection.get();

        if (connection == null)
            return null;

        return connection.client();
    }

    /**
     * prevent unnecessary writes if entry is already in cache. Integer is the
     * hashCode of the series.
     */
    private final Cache<Series, Boolean> writeCache = CacheBuilder.newBuilder()
            .concurrencyLevel(4).expireAfterWrite(15, TimeUnit.MINUTES).build();

    @Override
    public synchronized void start() throws Exception {
        if (this.connection.get() != null)
            return;

        final Connection connection = initConnection();

        try {
            initMapping(connection);
        } catch (final Exception e) {
            stop();
            throw new Exception("Failed to initialize mapping", e);
        }

        initBulkProcessor(connection);
    }

    private synchronized void initMapping(Connection connection)
            throws Exception {
        log.info("Setting up mapping for index={} and type={}", index, type);

        final Client client = connection.client();

        final AdminClient admin = client.admin();

        if (indexExists(admin))
            return;

        final IndicesAdminClient indices = admin.indices();

        createIndex(indices);
        createMapping(indices);
    }

    private boolean indexExists(AdminClient admin) {
        final ClusterStateResponse response = admin.cluster().prepareState()
                .get();

        return response.getState().metaData().hasIndex(index);
    }

    private void createMapping(final IndicesAdminClient indices)
            throws Exception {
        final PutMappingResponse response = indices.preparePutMapping(index)
                .setType(type).setSource(mapping).get();

        if (!response.isAcknowledged())
            throw new Exception("Failed to setup mapping: "
                    + response.toString());
    }

    private void createIndex(final IndicesAdminClient indices) throws Exception {
        final CreateIndexResponse response;

        try {
            response = indices.prepareCreate(index).get();
        } catch (final IndexAlreadyExistsException e) {
            log.info("Index already exists: {}", index);
            return;
        }

        if (!response.isAcknowledged())
            throw new Exception("Failed to setup index: " + response.toString());
    }

    private synchronized Connection initConnection()
            throws UnknownHostException, Exception {
        final Connection connection;

        if (nodeClient) {
            log.info("Starting (Node Client)");
            connection = setupNodeClient();
        } else {
            log.info("Starting (Transport Client)");
            connection = setupTransportClient();
        }

        if (this.connection.compareAndSet(null, connection))
            return connection;

        connection.close();
        return null;
    }

    private synchronized void initBulkProcessor(final Connection connection) {
        final Client client = connection.client();

        final Builder builder = BulkProcessor.builder(client, new Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    Throwable failure) {
                reporter.reportWriteFailure(request.numberOfActions());
                log.error("Failed to write bulk", failure);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    BulkResponse response) {
                reporter.reportWriteBatchDuration(response.getTookInMillis());

                final int all = response.getItems().length;

                if (!response.hasFailures()) {
                    reporter.reportWriteSuccess(all);
                    return;
                }

                final BulkItemResponse[] responses = response.getItems();
                int failures = 0;

                for (final BulkItemResponse r : responses) {
                    if (r.isFailed()) {
                        failures++;
                    }
                }

                reporter.reportWriteFailure(failures);
                reporter.reportWriteSuccess(all - failures);
            }
        });

        builder.setConcurrentRequests(concurrentBulkRequests);

        if (dumpInterval != null) {
            builder.setFlushInterval(new TimeValue(dumpInterval));
        }

        builder.setBulkSize(new ByteSizeValue(-1)); // Disable bulk size
        builder.setBulkActions(bulkActions);
        final BulkProcessor bulkProcessor = builder.build();

        if (!this.bulkProcessor.compareAndSet(null, bulkProcessor))
            bulkProcessor.close();

    }

    @Override
    public synchronized void stop() throws Exception {
        stopBulkProcessor();
        stopConnection();
    }

    private synchronized void stopBulkProcessor() {
        final BulkProcessor bulkProcessor = this.bulkProcessor.get();

        if (bulkProcessor == null)
            return;

        if (this.bulkProcessor.compareAndSet(bulkProcessor, null))
            bulkProcessor.close();
    }

    private synchronized void stopConnection() throws Exception {
        final Connection connection = this.connection.get();

        if (connection == null)
            return;

        if (this.connection.compareAndSet(connection, null))
            connection.close();
    }

    private synchronized Connection setupNodeClient()
            throws UnknownHostException {
        final Settings settings = ImmutableSettings
                .builder()
                .put("node.name", InetAddress.getLocalHost().getHostName())
                .put("discovery.zen.ping.multicast.enabled", false)
                .putArray("discovery.zen.ping.unicast.hosts",
                        seedsToDiscovery()).build();
        final Node node = NodeBuilder.nodeBuilder().settings(settings)
                .client(true).clusterName(clusterName).node();
        final Client client = node.client();

        return new Connection() {
            @Override
            public Client client() {
                return client;
            }

            @Override
            public void close() throws Exception {
                node.close();
            }
        };
    }

    private String[] seedsToDiscovery() {
        final List<String> seeds = new ArrayList<>();

        for (final InetSocketTransportAddress seed : this.seeds) {
            seeds.add(String.format("%s:%d", seed.address().getHostString(),
                    seed.address().getPort()));
        }

        return seeds.toArray(new String[0]);
    }

    private synchronized Connection setupTransportClient() {
        final Settings settings = ImmutableSettings.builder()
                .put("cluster.name", clusterName).build();

        final TransportClient client = new TransportClient(settings);

        for (final InetSocketTransportAddress seed : seeds) {
            client.addTransportAddress(seed);
        }

        return new Connection() {
            @Override
            public Client client() {
                return client;
            }

            @Override
            public void close() throws Exception {
                client.close();
            }
        };
    }

    @Override
    public Callback<FindTags> findTags(final Filter filter)
            throws MetadataOperationException {
        final Client client = client();

        if (client == null)
            throw new MetadataOperationException("Backend not ready");

        return findTagKeys(filter).transform(
                new FindTagsTransformer(pools.read(), client, index, type,
                        filter)).register(reporter.reportFindTags());
    }

    @Override
    public void write(String id, Series s) throws MetadataOperationException {
        final Client client = client();
        final BulkProcessor bulkProcessor = this.bulkProcessor.get();

        if (client == null || bulkProcessor == null)
            throw new MetadataOperationException("Backend not ready");

        try {
            writeSeries(client, bulkProcessor, id, s);
        } catch (final ExecutionException e) {
            throw new MetadataOperationException("Failed to write", e);
        }
    }

    @Override
    public Callback<FindSeries> findSeries(final Filter filter)
            throws MetadataOperationException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<>(CancelReason.BACKEND_DISABLED);

        return ConcurrentCallback.newResolve(pools.read(),
                new FindSeriesResolver(client, index, type, filter)).register(
                reporter.reportFindTimeSeries());
    }

    @Override
    public Callback<DeleteSeries> deleteSeries(final Filter filter)
            throws MetadataOperationException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<>(CancelReason.BACKEND_DISABLED);

        return ConcurrentCallback.newResolve(pools.write(),
                new DeleteTimeSeriesResolver(client, index, type, filter));
    }

    public Callback<FindTagKeys> findTagKeys(final Filter filter)
            throws MetadataOperationException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindTagKeys>(
                    CancelReason.BACKEND_DISABLED);

        return ConcurrentCallback.newResolve(pools.read(),
                new FindTagKeysResolver(client, index, type, filter)).register(
                reporter.reportFindTagKeys());
    }

    @Override
    public Callback<FindKeys> findKeys(final Filter filter)
            throws MetadataOperationException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindKeys>(
                    CancelReason.BACKEND_DISABLED);

        return ConcurrentCallback.newResolve(pools.read(),
                new FindKeysResolver(client, index, type, filter)).register(
                reporter.reportFindKeys());
    }

    @Override
    public Callback<WriteBatchResult> write(final Series series)
            throws MetadataOperationException {
        return writeBatch(Arrays.asList(series));
    }

    @Override
    public Callback<WriteBatchResult> writeBatch(final List<Series> series)
            throws MetadataOperationException {
        final Client client = client();
        final BulkProcessor bulkProcessor = this.bulkProcessor.get();

        if (client == null || bulkProcessor == null)
            throw new MetadataOperationException("Not ready");

        for (final Series s : series) {
            final String id = MetadataUtils.buildId(s);

            try {
                writeSeries(client, bulkProcessor, id, s);
            } catch (final ExecutionException e) {
                throw new MetadataOperationException("Failed to write", e);
            }
        }

        return new ResolvedCallback<WriteBatchResult>(new WriteBatchResult(
                true, 1));
    }

    private void writeSeries(final Client client,
            final BulkProcessor bulkProcessor, final String id, final Series s)
            throws MetadataOperationException, ExecutionException {
        writeCache.get(s, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                reporter.reportWriteCacheMiss();

                final Map<String, Object> source = buildSeries(s);
                final IndexRequest request = client.prepareIndex()
                        .setIndex(index).setType(type).setId(id)
                        .setSource(source).request();

                bulkProcessor.add(request);
                return true;
            }
        });
    }

    @Override
    public Callback<Void> refresh() {
        return new ResolvedCallback<Void>(null);
    }

    @Override
    public boolean isReady() {
        return client() != null && bulkProcessor.get() != null;
    }

    public static Series toSeries(Map<String, Object> source) {
        final Map<String, String> tags = extractTags(source);
        final String key = (String) source.get("key");
        return new Series(key, tags);
    }

    public static Map<String, Object> buildSeries(Series series) {
        final Map<String, Object> source = new HashMap<String, Object>();
        source.put("key", series.getKey());
        source.put("tags", buildTags(series.getTags()));
        return source;
    }

    private static List<Map<String, String>> buildTags(Map<String, String> map) {
        final List<Map<String, String>> tags = new ArrayList<Map<String, String>>();

        if (map == null || map.isEmpty())
            return tags;

        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final Map<String, String> tag = new HashMap<String, String>();
            tag.put("key", entry.getKey());
            tag.put("value", entry.getValue());
            tags.add(tag);
        }

        return tags;
    }

    private static Map<String, String> extractTags(
            final Map<String, Object> source) {
        @SuppressWarnings("unchecked")
        final List<Map<String, String>> attributes = (List<Map<String, String>>) source
                .get("tags");
        final Map<String, String> tags = new HashMap<String, String>();

        for (final Map<String, String> entry : attributes) {
            final String key = entry.get("key");
            final String value = entry.get("value");
            tags.put(key, value);
        }

        return tags;
    }
}

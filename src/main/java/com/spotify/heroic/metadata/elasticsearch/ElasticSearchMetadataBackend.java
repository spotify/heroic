package com.spotify.heroic.metadata.elasticsearch;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.elasticsearch.async.FindKeysResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindTagKeysResolver;
import com.spotify.heroic.metadata.elasticsearch.async.FindTagsTransformer;
import com.spotify.heroic.metadata.elasticsearch.async.FindTimeSeriesResolver;
import com.spotify.heroic.metadata.elasticsearch.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
@Slf4j
public class ElasticSearchMetadataBackend implements MetadataBackend {
    public static final String TAGS_VALUE = "tags.value";
    public static final String TAGS_KEY = "tags.key";
    public static final String TAGS = "tags";
    public static final String KEY = "key";

    /**
     * 
     */
    public static class YAML implements MetadataBackend.YAML {
        public static String TYPE = "!elasticsearch-metadata";

        @Getter
        @Setter
        private List<String> seeds;

        @Getter
        @Setter
        private String clusterName = "elasticsearch";

        @Getter
        @Setter
        private String index = "heroic";

        @Getter
        @Setter
        private String type = "metadata";

        @Getter
        @Setter
        private boolean nodeClient = false;

        @Getter
        @Setter
        private int writeThreads = 20;

        @Getter
        @Setter
        private int writeQueueSize = 1000;

        @Override
        public MetadataBackend build(String context,
                MetadataBackendReporter reporter) throws ValidationException {
            final String[] seeds = this.seeds.toArray(new String[this.seeds
                    .size()]);
            final Executor executor = Executors.newFixedThreadPool(10);

            final ThreadPoolExecutor writeExecutor = new ThreadPoolExecutor(
                    writeThreads, writeThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(writeQueueSize));

            reporter.newWriteThreadPool(new ThreadPoolProvider() {
                @Override
                public int getQueueSize() {
                    return writeExecutor.getQueue().size();
                }
            });

            return new ElasticSearchMetadataBackend(executor, writeExecutor,
                    reporter, seeds, clusterName, index, type, nodeClient);
        }
    }

    /**
     * Common connection abstraction between Node and TransportClient.
     */
    private interface Connection {
        Client client();

        void close() throws Exception;
    }

    private final Executor executor;
    private final ThreadPoolExecutor writeExecutor;
    private final MetadataBackendReporter reporter;
    private final String[] seeds;
    private final String clusterName;
    private final String index;
    private final String type;
    private final boolean nodeClient;

    private final AtomicReference<Connection> connection = new AtomicReference<Connection>();

    public Client client() {
        final Connection connection = this.connection.get();

        if (connection == null)
            return null;

        return connection.client();
    }

    /**
     * prevent unnecessary writes if entry is already in cache. Integer is the
     * hashCode of the timeSerie.
     */
    private final Set<Integer> writeCache = Collections
            .newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    @Override
    public synchronized void start() throws Exception {
        if (this.connection.get() != null)
            return;

        final Connection connection;

        if (nodeClient) {
            log.info("Starting (Node Client)");
            connection = setupNodeClient();
        } else {
            log.info("Starting (Transport Client)");
            connection = setupTransportClient();
        }

        if (!this.connection.compareAndSet(null, connection))
            connection.close();
    }

    @Override
    public synchronized void stop() throws Exception {
        final Connection connection = this.connection.get();

        if (connection == null)
            return;

        if (this.connection.compareAndSet(connection, null))
            connection.close();
    }

    private Connection setupNodeClient() throws UnknownHostException {
        final Settings settings = ImmutableSettings.builder()
                .put("node.name", InetAddress.getLocalHost().getHostName())
                .put("discovery.zen.ping.multicast.enabled", false)
                .putArray("discovery.zen.ping.unicast.hosts", seeds).build();
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

    private Connection setupTransportClient() {
        final Settings settings = ImmutableSettings.builder()
                .put("cluster.name", clusterName).build();

        final TransportClient client = new TransportClient(settings);

        for (final String seed : seeds) {
            final InetSocketTransportAddress transportAddress;

            try {
                transportAddress = parseInetSocketTransportAddress(seed);
            } catch (Exception e) {
                log.error("Invalid seed address: {}", seed, e);
                continue;
            }

            client.addTransportAddress(transportAddress);
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

    private InetSocketTransportAddress parseInetSocketTransportAddress(
            final String seed) throws URISyntaxException {
        if (seed.contains(":")) {
            final String parts[] = seed.split(":");
            return new InetSocketTransportAddress(parts[0],
                    Integer.valueOf(parts[1]));
        }

        return new InetSocketTransportAddress(seed, 9300);
    }

    @Override
    public Callback<FindTags> findTags(final TimeSerieQuery query,
            final Set<String> includes, final Set<String> excludes)
            throws MetadataQueryException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindTags>(
                    CancelReason.BACKEND_DISABLED);

        return findTagKeys(query).transform(
                new FindTagsTransformer(executor, client, index, type, query,
                        includes, excludes))
                .register(reporter.reportFindTags());
    }

    @Override
    public Callback<FindTimeSeries> findTimeSeries(final TimeSerieQuery query)
            throws MetadataQueryException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindTimeSeries>(
                    CancelReason.BACKEND_DISABLED);

        final QueryBuilder builder = toQueryBuilder(query);

        return ConcurrentCallback.newResolve(executor,
                new FindTimeSeriesResolver(client, index, type, builder))
                .register(reporter.reportFindTimeSeries());
    }

    public Callback<FindTagKeys> findTagKeys(final TimeSerieQuery query)
            throws MetadataQueryException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindTagKeys>(
                    CancelReason.BACKEND_DISABLED);

        final QueryBuilder builder = toQueryBuilder(query);

        return ConcurrentCallback.newResolve(executor,
                new FindTagKeysResolver(client, index, type, builder))
                .register(reporter.reportFindTagKeys());
    }

    @Override
    public Callback<FindKeys> findKeys(final TimeSerieQuery query)
            throws MetadataQueryException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<FindKeys>(
                    CancelReason.BACKEND_DISABLED);

        final QueryBuilder builder = toQueryBuilder(query);

        return ConcurrentCallback.newResolve(executor,
                new FindKeysResolver(client, index, type, builder)).register(
                reporter.reportFindKeys());
    }

    @Override
    public Callback<WriteResponse> write(final TimeSerie timeSerie)
            throws MetadataQueryException {
        final Client client = client();

        if (client == null)
            return new CancelledCallback<WriteResponse>(
                    CancelReason.BACKEND_DISABLED);

        if (writeCache.contains(timeSerie)) {
            reporter.reportWriteCacheHit();
            return new ResolvedCallback<WriteResponse>(new WriteResponse(1, 0));
        }

        reporter.reportWriteCacheMiss();

        final Map<String, Object> source = fromTimeSerie(timeSerie);

        return ConcurrentCallback.newResolve(writeExecutor,
                new Callback.Resolver<WriteResponse>() {
                    @Override
                    public WriteResponse resolve() throws Exception {
                        final String id = Integer.toHexString(timeSerie
                                .hashCode());

                        final ListenableActionFuture<IndexResponse> future = client
                                .prepareIndex().setIndex(index).setType(type)
                                .setId(id).setSource(source).execute();

                        future.get();

                        writeCache.add(timeSerie.hashCode());
                        return new WriteResponse(1, 0);
                    }
                }).register(reporter.reportWrite());
    }

    @Override
    public Callback<Void> refresh() {
        return new ResolvedCallback<Void>(null);
    }

    @Override
    public boolean isReady() {
        return client() != null;
    }

    public static QueryBuilder toQueryBuilder(final TimeSerieQuery query) {
        boolean any = false;
        final BoolQueryBuilder builder = QueryBuilders.boolQuery();

        if (query.getMatchKey() != null) {
            any = true;
            builder.must(QueryBuilders.termQuery("key", query.getMatchKey()));
        }

        /**
         * On empty, match nothing (hopefully).
         */
        if (query.getMatchTags() != null) {
            for (Map.Entry<String, String> entry : query.getMatchTags()
                    .entrySet()) {
                any = true;
                builder.must(QueryBuilders.nestedQuery(
                        TAGS,
                        QueryBuilders
                                .boolQuery()
                                .must(QueryBuilders.termQuery(TAGS_KEY,
                                        entry.getKey()))
                                .must(QueryBuilders.termQuery(TAGS_VALUE,
                                        entry.getValue()))));
            }
        }

        /**
         * On empty, match nothing.
         */
        if (query.getHasTags() != null) {
            for (String key : query.getHasTags()) {
                any = true;
                builder.must(QueryBuilders.nestedQuery(TAGS,
                        QueryBuilders.termQuery(TAGS_KEY, key)));
            }
        }

        if (!any)
            return null;

        return builder;
    }

    public static Map<String, Object> fromTimeSerie(TimeSerie timeSerie) {
        final Map<String, Object> source = new HashMap<String, Object>();
        source.put("key", timeSerie.getKey());
        source.put("tags", buildTags(timeSerie.getTags()));
        return source;
    }

    public static List<Map<String, String>> buildTags(Map<String, String> map) {
        final List<Map<String, String>> tags = new ArrayList<Map<String, String>>();

        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final Map<String, String> tag = new HashMap<String, String>();
            tag.put("key", entry.getKey());
            tag.put("value", entry.getValue());
            tags.add(tag);
        }

        return tags;
    }

    public static TimeSerie toTimeSerie(Map<String, Object> source) {
        final Map<String, String> tags = extractTags(source);
        final String key = (String) source.get("key");
        return new TimeSerie(key, tags);
    }

    public static Map<String, String> extractTags(
            final Map<String, Object> source) {
        @SuppressWarnings("unchecked")
        final List<Map<String, String>> attributes = (List<Map<String, String>>) source
                .get("tags");
        final Map<String, String> tags = new HashMap<String, String>();

        for (Map<String, String> entry : attributes) {
            final String key = entry.get("key");
            final String value = entry.get("value");
            tags.put(key, value);
        }

        return tags;
    }
}

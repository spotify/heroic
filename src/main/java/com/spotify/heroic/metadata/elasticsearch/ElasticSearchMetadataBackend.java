package com.spotify.heroic.metadata.elasticsearch;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
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
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
@ToString(of = { "seeds", "clusterName", "index", "type", "nodeClient",
		"bulkActions", "dumpInterval", "concurrentBulkRequests" })
public class ElasticSearchMetadataBackend implements MetadataBackend {
	public static final String TAGS_VALUE = "tags.value";
	public static final String TAGS_KEY = "tags.key";
	public static final String TAGS = "tags";
	public static final String KEY = "key";

	@Data
	public static class YAML implements MetadataBackend.YAML {
		public static String TYPE = "!elasticsearch-metadata";

		private List<String> seeds;
		private String clusterName = "elasticsearch";
		private String index = "heroic";
		private String type = "metadata";
		private boolean nodeClient = false;

		private int readThreads = 20;
		private int readQueueSize = 10000;

		private int writeThreads = 20;
		private int writeQueueSize = 10000;
		private int writeBulkActions = 1000;
		private Long writeBulkFlushInterval = null;

		private int concurrentBulkRequests = 5;

		@Override
		public MetadataBackend build(String context,
				MetadataBackendReporter reporter) throws ValidationException {
			final String[] seeds = this.seeds.toArray(new String[this.seeds
			                                                     .size()]);

			final ReadWriteThreadPools pools = ReadWriteThreadPools.config()
					.readThreads(readThreads).readQueueSize(readQueueSize)
					.writeThreads(writeThreads).writeQueueSize(writeQueueSize)
					.build();

			return new ElasticSearchMetadataBackend(pools, reporter, seeds,
					clusterName, index, type, nodeClient, writeBulkActions,
					writeBulkFlushInterval, concurrentBulkRequests);
		}
	}

	/**
	 * Common connection abstraction between Node and TransportClient.
	 */
	private interface Connection {
		Client client();

		void close() throws Exception;
	}

	private final ReadWriteThreadPools pools;
	private final MetadataBackendReporter reporter;
	private final String[] seeds;
	private final String clusterName;
	private final String index;
	private final String type;
	private final boolean nodeClient;
	private final int bulkActions;
	private final Long dumpInterval;
	private final int concurrentBulkRequests;

	private final AtomicReference<Connection> connection = new AtomicReference<Connection>();
	private final AtomicReference<BulkProcessor> bulkProcessorRef = new AtomicReference<BulkProcessor>();

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

		initializeConnection();
		initializeBulkProcessor();
	}

	private void initializeConnection() throws UnknownHostException, Exception {
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

	private void initializeBulkProcessor() {
		final Client client = client();
		if (client == null)
			return;

		final Builder builder = BulkProcessor.builder(client, new Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				reporter.reportWriteFailure(request.numberOfActions());
				log.error(
						"An error occured during Elasticsearch bulk execution.",
						failure);
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
		if (!this.bulkProcessorRef.compareAndSet(null, bulkProcessor))
			bulkProcessor.close();

	}

	@Override
	public synchronized void stop() throws Exception {
		stopBulkProcessor();
		stopConnection();
	}

	private void stopBulkProcessor() {
		final BulkProcessor bulkProcessor = this.bulkProcessorRef.get();
		if (bulkProcessor == null)
			return;

		if (this.bulkProcessorRef.compareAndSet(bulkProcessor, null))
			bulkProcessor.close();
	}

	private void stopConnection() throws Exception {
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
			} catch (final Exception e) {
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
				new FindTagsTransformer(pools.read(), client, index, type,
						query, includes, excludes)).register(
								reporter.reportFindTags());
	}

	@Override
	public Callback<FindTimeSeries> findTimeSeries(final TimeSerieQuery query)
			throws MetadataQueryException {
		final Client client = client();

		if (client == null)
			return new CancelledCallback<FindTimeSeries>(
					CancelReason.BACKEND_DISABLED);

		final QueryBuilder builder = toQueryBuilder(query);

		return ConcurrentCallback.newResolve(pools.read(),
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

		return ConcurrentCallback.newResolve(pools.read(),
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

		return ConcurrentCallback.newResolve(pools.read(),
				new FindKeysResolver(client, index, type, builder)).register(
						reporter.reportFindKeys());
	}

	@Override
	public Callback<WriteResponse> write(final TimeSerie timeSerie)
			throws MetadataQueryException {
		return writeBatch(Arrays.asList(timeSerie));
	}

	@Override
	public Callback<WriteResponse> writeBatch(final List<TimeSerie> timeSeries)
			throws MetadataQueryException {
		final Client client = client();
		final BulkProcessor bulkProcessor = this.bulkProcessorRef.get();
		if (client == null || bulkProcessor == null)
			return new CancelledCallback<WriteResponse>(
					CancelReason.BACKEND_DISABLED);

		return ConcurrentCallback.newResolve(pools.write(),
				new Callback.Resolver<WriteResponse>() {
			@Override
			public WriteResponse resolve() throws Exception {
				for (final TimeSerie timeSerie : timeSeries) {
					if (writeCache.contains(timeSerie.hashCode())) {
						reporter.reportWriteCacheHit();
						continue;
					}

					reporter.reportWriteCacheMiss();

					final Map<String, Object> source = fromTimeSerie(timeSerie);
					final IndexRequest request = client.prepareIndex()
							.setIndex(index).setType(type)
							.setSource(source).request();
					bulkProcessor.add(request);

					writeCache.add(timeSerie.hashCode());
				}
				return new WriteResponse(timeSeries.size(), 0, 0);
			}
		}).register(reporter.reportWrite());
	}

	@Override
	public Callback<Void> refresh() {
		return new ResolvedCallback<Void>(null);
	}

	@Override
	public boolean isReady() {
		return client() != null && bulkProcessorRef.get() != null;
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
			for (final Map.Entry<String, String> entry : query.getMatchTags()
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
			for (final String key : query.getHasTags()) {
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

		for (final Map<String, String> entry : attributes) {
			final String key = entry.get("key");
			final String value = entry.get("value");
			tags.put(key, value);
		}

		return tags;
	}
}

package com.spotify.heroic.metadata.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.metadata.FilteringTimeSerieMatcher;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.TimeSerieMatcher;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
@Slf4j
public class ElasticSearchMetadataBackend implements MetadataBackend, Startable {
    private static final String ATTRIBUTES_VALUE = "attributes.value";
    private static final String ATTRIBUTES_KEY = "attributes.key";
    private static final String ATTRIBUTES = "attributes";

    /**
     * host - key - attribute - tags -
     */
    public static class YAML implements MetadataBackend.YAML {
        public static String TYPE = "!elasticsearch-metadata";

        @Getter
        @Setter
        private List<String> seeds;

        @Getter
        @Setter
        private String clusterName = "elasticsearch";

        @Override
        public MetadataBackend build(String context,
                MetadataBackendReporter reporter) throws ValidationException {
            final String[] seeds = this.seeds.toArray(new String[this.seeds
                    .size()]);
            final Executor executor = Executors.newFixedThreadPool(10);
            return new ElasticSearchMetadataBackend(reporter, seeds,
                    clusterName, executor);
        }
    }

    private final MetadataBackendReporter reporter;
    private final String[] seeds;
    private final String clusterName;
    private final String index = "heroic";
    private final String type = "metadata";
    private final Executor executor;

    private Node node;
    private Client client;

    @Override
    @PostConstruct
    public void start() throws Exception {
        log.info("Starting");

        final Settings settings = ImmutableSettings.builder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .putArray("discovery.zen.ping.unicast.hosts", seeds).build();

        this.node = NodeBuilder.nodeBuilder().settings(settings).client(true)
                .clusterName(clusterName).node();
        this.client = node.client();
    }

    @Override
    public Callback<FindTags> findTags(final TimeSerieMatcher matcher,
            final Set<String> includes, final Set<String> excludes)
            throws MetadataQueryException {
        if (node == null)
            throw new MetadataQueryException("Node not started");

        return findKeys(matcher).transform(
                new Callback.DeferredTransformer<FindKeys, FindTags>() {
                    @Override
                    public Callback<FindTags> transform(FindKeys result)
                            throws Exception {
                        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

                        for (final String key : result.getKeys()) {
                            if (includes != null && !includes.contains(key))
                                continue;

                            if (excludes != null && excludes.contains(key))
                                continue;

                            callbacks.add(findSingle(matcher, key));
                        }

                        return ConcurrentCallback.newReduce(callbacks,
                                new FindTagsReducer());
                    }

                    /**
                     * Finds a single set of tags, excluding any criteria for
                     * this specific set of tags.
                     * 
                     * @param matcher
                     * @param key
                     * @return
                     */
                    private Callback<FindTags> findSingle(
                            final TimeSerieMatcher matcher, final String key) {
                        final TimeSerieMatcher newMatcher;

                        if (matcher.matchTags() == null) {
                            newMatcher = matcher;
                        } else {
                            final Map<String, String> newMatchTags = new HashMap<String, String>(
                                    matcher.matchTags());
                            newMatchTags.remove(key);
                            newMatcher = new FilteringTimeSerieMatcher(matcher
                                    .matchKey(), newMatchTags, matcher
                                    .hasTags());
                        }

                        final QueryBuilder query = setupTimeSeriesQuery(newMatcher);

                        return ConcurrentCallback.newResolve(executor,
                                new Callback.Resolver<FindTags>() {
                                    @Override
                                    public FindTags resolve() throws Exception {
                                        final SearchRequestBuilder request = client
                                                .prepareSearch(index)
                                                .setTypes(type)
                                                .setSearchType("count");

                                        if (query != null) {
                                            request.setQuery(query);
                                        }

                                        {
                                            final AggregationBuilder<?> terms = AggregationBuilders
                                                    .terms("terms").field(
                                                            ATTRIBUTES_VALUE);
                                            final AggregationBuilder<?> filter = AggregationBuilders
                                                    .filter("filter")
                                                    .filter(FilterBuilders
                                                            .termFilter(
                                                                    ATTRIBUTES_KEY,
                                                                    key))
                                                    .subAggregation(terms);
                                            final AggregationBuilder<?> aggregation = AggregationBuilders
                                                    .nested("nested")
                                                    .path(ATTRIBUTES)
                                                    .subAggregation(filter);
                                            request.addAggregation(aggregation);
                                        }

                                        final SearchResponse response = request
                                                .get();

                                        final Terms terms;

                                        /*
                                         * IMPORTANT: has to be unwrapped with
                                         * the correct type in the correct order
                                         * as specified above!
                                         */
                                        {
                                            final Aggregations aggregations = response
                                                    .getAggregations();
                                            final Nested attributes = (Nested) aggregations
                                                    .get("nested");
                                            final Filter filter = (Filter) attributes
                                                    .getAggregations().get(
                                                            "filter");
                                            terms = (Terms) filter
                                                    .getAggregations().get(
                                                            "terms");
                                        }

                                        final Set<String> values = new HashSet<String>();

                                        for (final Terms.Bucket bucket : terms
                                                .getBuckets()) {
                                            values.add(bucket.getKey());
                                        }

                                        final Map<String, Set<String>> result = new HashMap<String, Set<String>>();
                                        result.put(key, values);
                                        return new FindTags(result, result
                                                .size());
                                    }
                                });
                    }
                }).register(reporter.reportFindTags());
    }

    @Override
    public Callback<FindTimeSeries> findTimeSeries(
            final TimeSerieMatcher matcher) throws MetadataQueryException {
        if (node == null)
            throw new MetadataQueryException("Node not started");

        final QueryBuilder query = setupTimeSeriesQuery(matcher);

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<FindTimeSeries>() {
                    @Override
                    public FindTimeSeries resolve() throws Exception {
                        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();

                        for (final SearchResponse response : setupFindTimeSeries(
                                matcher, query)) {
                            for (final SearchHit hit : response.getHits()) {
                                timeSeries.add(hitToTimeSerie(hit));
                            }
                        }

                        return new FindTimeSeries(timeSeries, timeSeries.size());
                    }
                }).register(reporter.reportFindTimeSeries());
    }

    @Override
    public Callback<FindKeys> findKeys(final TimeSerieMatcher matcher)
            throws MetadataQueryException {
        if (node == null)
            throw new MetadataQueryException("Node not started");

        final QueryBuilder query = setupTimeSeriesQuery(matcher);

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<FindKeys>() {
                    @Override
                    public FindKeys resolve() throws Exception {
                        final SearchRequestBuilder request = client
                                .prepareSearch(index).setTypes(type)
                                .setSearchType("count");

                        if (query != null) {
                            request.setQuery(query);
                        }

                        {
                            final AggregationBuilder<?> terms = AggregationBuilders
                                    .terms("terms").field(ATTRIBUTES_KEY);
                            final AggregationBuilder<?> nested = AggregationBuilders
                                    .nested("nested").path(ATTRIBUTES)
                                    .subAggregation(terms);
                            request.addAggregation(nested);
                        }

                        final SearchResponse response = request.get();

                        final Terms terms;

                        {
                            final Aggregations aggregations = response
                                    .getAggregations();
                            final Nested attributes = (Nested) aggregations
                                    .get("nested");
                            terms = (Terms) attributes.getAggregations().get(
                                    "terms");
                        }

                        final Set<String> keys = new HashSet<String>();

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
                            keys.add(bucket.getKey());
                        }

                        return new FindKeys(keys, keys.size());
                    }
                }).register(reporter.reportFindTimeSeries());
    }

    private Iterable<SearchResponse> setupFindTimeSeries(
            final TimeSerieMatcher matcher, final QueryBuilder query) {
        return new Iterable<SearchResponse>() {
            @Override
            public Iterator<SearchResponse> iterator() {
                return new Iterator<SearchResponse>() {
                    private SearchResponse next;
                    private int from = 0;
                    private final int size = 100;

                    @Override
                    public boolean hasNext() {
                        final SearchRequestBuilder request = client
                                .prepareSearch(index).setTypes(type)
                                .setFrom(from).setSize(size);

                        if (query != null) {
                            request.setQuery(query);
                        }

                        final SearchResponse next = request.get();

                        if (next.getHits().getHits().length == 0)
                            return false;

                        this.from += this.size;
                        this.next = next;
                        return true;
                    }

                    @Override
                    public SearchResponse next() {
                        final SearchResponse next = this.next;
                        this.next = null;
                        return next;
                    }

                    @Override
                    public void remove() {
                    };
                };
            }
        };
    }

    private QueryBuilder setupTimeSeriesQuery(final TimeSerieMatcher matcher) {
        boolean any = false;
        final BoolQueryBuilder query = QueryBuilders.boolQuery();

        if (matcher.matchKey() != null) {
            any = true;
            query.must(QueryBuilders.termQuery("key", matcher.matchKey()));
        }

        if (matcher.matchTags() != null) {
            any = true;

            for (Map.Entry<String, String> entry : matcher.matchTags()
                    .entrySet()) {
                if (entry.getKey().equals("host")) {
                    query.must(QueryBuilders.termQuery("host", entry.getValue()));
                    continue;
                }

                final BoolQueryBuilder tagQuery = QueryBuilders.boolQuery();
                tagQuery.must(QueryBuilders.termQuery(ATTRIBUTES_KEY,
                        entry.getKey()));
                tagQuery.must(QueryBuilders.termQuery("attributes.value",
                        entry.getValue()));
                query.must(QueryBuilders.nestedQuery(ATTRIBUTES, tagQuery));
            }
        }

        if (matcher.hasTags() != null) {
            for (String key : matcher.hasTags()) {
                if (key.equals("host"))
                    continue;

                any = true;
                query.must(QueryBuilders.nestedQuery(ATTRIBUTES,
                        QueryBuilders.termQuery(ATTRIBUTES_KEY, key)));
            }
        }

        if (!any)
            return null;

        return query;
    }

    private TimeSerie hitToTimeSerie(SearchHit hit) {
        final Map<String, Object> source = hit.getSource();
        final Map<String, String> tags = extractTags(source);
        final String key = (String) source.get("key");
        return new TimeSerie(key, tags);
    }

    private Map<String, String> extractTags(final Map<String, Object> source) {
        @SuppressWarnings("unchecked")
        final List<Map<String, String>> attributes = (List<Map<String, String>>) source
                .get(ATTRIBUTES);
        final Map<String, String> tags = new HashMap<String, String>();

        for (Map<String, String> entry : attributes) {
            final String key = entry.get("key");
            final String value = entry.get("value");
            tags.put(key, value);
        }

        final String host = (String) source.get("host");
        tags.put("host", host);
        return tags;
    }

    @Override
    public Callback<Void> refresh() {
        return new ResolvedCallback<Void>(null);
    }

    @Override
    public boolean isReady() {
        return true;
    }
}

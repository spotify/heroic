package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.ShardedResultGroup;
import eu.toolchain.async.AsyncFuture;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.spotify.heroic.test.Data.points;
import static org.junit.Assert.assertEquals;

public class ClusterQueryIT extends AbstractLocalClusterIT {
    private final Series s1 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "a"));
    private final Series s2 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "b"));

    private QueryManager query;

    @Before
    public void setup() {
        query = instances.get(0).inject(CoreComponent::queryManager);
    }

    @Override
    protected AsyncFuture<Void> prepareEnvironment() {
        final List<IngestionManager> ingestion = instances
            .stream()
            .map(i -> i.inject(IngestionComponent::ingestionManager))
            .collect(Collectors.toList());

        final List<AsyncFuture<Ingestion>> writes = new ArrayList<>();

        final IngestionManager m1 = ingestion.get(0);
        final IngestionManager m2 = ingestion.get(1);

        writes.add(m1
            .useDefaultGroup()
            .write(new Ingestion.Request(s1, points().p(10, 1D).p(30, 2D).build())));
        writes.add(m2
            .useDefaultGroup()
            .write(new Ingestion.Request(s2, points().p(10, 1D).p(20, 4D).build())));

        return async.collectAndDiscard(writes);
    }

    public QueryResult query(final String queryString) throws Exception {
        final Query q = query
            .newQueryFromString(queryString)
            .features(Features.of(Feature.DISTRIBUTED_AGGREGATIONS))
            .source(Optional.of(MetricType.POINT))
            .rangeIfAbsent(Optional.of(new QueryDateRange.Absolute(10, 40)))
            .build();

        return query.useDefaultGroup().query(q).get();
    }

    @Test
    public void basicQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 4D).p(30, 2D).build()), m);
    }

    @Test
    public void distributedQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by shared");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 4D).p(30, 2D).build()), m);
    }

    @Test
    public void distributedDifferentQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by diff");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L, 10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(30, 2D).build(),
            points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void filterQueryTest() throws Exception {
        final QueryResult result = query("average(10ms) by * | topk(2) | bottomk(1) | sum(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void filterLastQueryTest() throws Exception {
        final QueryResult result = query("average(10ms) by * | topk(2) | bottomk(1)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 4D).build()), m);
    }

    @Test
    public void cardinalityTest() throws Exception {
        final QueryResult result = query("cardinality(10ms)");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 1D).p(20, 1D).p(30, 1D).p(40, 0D).build()), m);
    }

    @Test
    public void cardinalityWithKeyTest() throws Exception {
        // TODO: support native booleans in expressions
        final QueryResult result = query("cardinality(10ms, method=hllp(includeKey=\"true\"))");

        final Set<MetricCollection> m = getResults(result);
        final List<Long> cadences = getCadences(result);

        assertEquals(ImmutableList.of(10L), cadences);
        assertEquals(ImmutableSet.of(points().p(10, 2D).p(20, 1D).p(30, 1D).p(40, 0D).build()), m);
    }

    private Set<MetricCollection> getResults(final QueryResult result) {
        return result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getMetrics)
            .collect(Collectors.toSet());
    }

    private List<Long> getCadences(final QueryResult result) {
        final List<Long> cadences = result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getCadence)
            .collect(Collectors.toList());

        Collections.sort(cadences);
        return cadences;
    }
}

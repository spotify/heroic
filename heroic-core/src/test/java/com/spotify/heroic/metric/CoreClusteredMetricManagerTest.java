package com.spotify.heroic.metric;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;

import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@RunWith(MockitoJUnitRunner.class)
public class CoreClusteredMetricManagerTest {
    @Rule
    public ExpectedException except = ExpectedException.none();

    @Mock
    private AsyncFramework async;
    @Mock
    private ClusteredMetricManagerReporter reporter;
    @Mock
    private ClusterManager cluster;
    @Mock
    private FilterFactory filters;
    @Mock
    private AggregationFactory aggregations;
    @Mock
    private QueryParser parser;
    @Mock
    private AsyncFuture<MetricResult> future;

    private CoreClusteredMetricManager underTest;

    @Before
    public void setup() {
        underTest = spy(new CoreClusteredMetricManager());
        underTest.async = async;
        underTest.reporter = reporter;
        underTest.cluster = cluster;
        underTest.filters = filters;
        underTest.aggregations = aggregations;
        underTest.parser = parser;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testQueryEmptyNodes() {
        except.expect(IllegalStateException.class);
        except.expectMessage("no query nodes found");

        final MetricQuery request = mock(MetricQuery.class);
        final Collection<NodeRegistryEntry> nodes = mock(Collection.class);

        doReturn(true).when(nodes).isEmpty();
        doReturn(nodes).when(cluster).findAllShards(NodeCapability.QUERY);
        underTest.query(request);

        final InOrder order = inOrder(nodes, cluster, underTest);
        order.verify(cluster).findAllShards(NodeCapability.QUERY);
        order.verify(nodes).isEmpty();
        order.verify(underTest, never()).doQuery(request, nodes);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testQuery() {
        final MetricQuery request = mock(MetricQuery.class);
        final Collection<NodeRegistryEntry> nodes = mock(Collection.class);

        doReturn(false).when(nodes).isEmpty();
        doReturn(nodes).when(cluster).findAllShards(NodeCapability.QUERY);
        doReturn(future).when(underTest).doQuery(request, nodes);
        underTest.query(request);

        final InOrder order = inOrder(nodes, cluster, underTest);
        order.verify(cluster).findAllShards(NodeCapability.QUERY);
        order.verify(nodes).isEmpty();
        order.verify(underTest).doQuery(request, nodes);
    }
}
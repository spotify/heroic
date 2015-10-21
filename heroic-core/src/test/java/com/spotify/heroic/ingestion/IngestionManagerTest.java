package com.spotify.heroic.ingestion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.spotify.heroic.common.BackendGroupException;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;

@RunWith(MockitoJUnitRunner.class)
public class IngestionManagerTest {
    private static final String group = "group";
    private static final int MAX_CONCURRENT_WRITES = 10000;

    @Mock
    private AsyncFuture<WriteResult> writeResult;
    @Mock
    private AsyncFramework async;
    @Mock
    private MetricManager metricManager;
    @Mock
    private MetadataManager metadataManager;
    @Mock
    private SuggestManager suggestManager;
    @Mock
    private IngestionManagerReporter reporter;
    @Mock
    private WriteMetric write;
    @Mock
    private AsyncFuture<WriteResult> future;
    @Mock
    private MetricBackendGroup metric;
    @Mock
    private MetadataBackend metadata;
    @Mock
    private SuggestBackend suggest;

    @Before
    public void setup() throws BackendGroupException {
        doReturn(metric).when(metricManager).useGroup(group);
        doReturn(metadata).when(metadataManager).useGroup(group);
        doReturn(suggest).when(suggestManager).useGroup(group);
        doReturn(future).when(future).onFinished(any());
    }

    private IngestionManagerImpl setupIngestionManager(final boolean updateMetrics, final boolean updateMetadata,
            final boolean updateSuggestions) {
        final IngestionManagerImpl manager = new IngestionManagerImpl(updateMetrics, updateMetadata, updateSuggestions, MAX_CONCURRENT_WRITES, TrueFilterImpl.get());
        manager.async = async;
        manager.metadata = metadataManager;
        manager.metric = metricManager;
        manager.suggest = suggestManager;
        manager.reporter = reporter;
        return spy(manager);
    }

    @Test
    public void testWriteEmpty() throws BackendGroupException {
        final IngestionManagerImpl manager = setupIngestionManager(true, true, true);

        doReturn(true).when(write).isEmpty();
        doReturn(future).when(async).resolved(any(WriteResult.class));

        assertEquals(future, manager.write(group, write));

        verify(async).resolved(any(WriteResult.class));
        verify(write).isEmpty();
        verify(manager, never()).syncWrite(group, write);
    }

    @Test
    public void testWrite() throws BackendGroupException {
        final IngestionManagerImpl manager = setupIngestionManager(true, true, true);

        doReturn(false).when(write).isEmpty();
        doReturn(future).when(manager).syncWrite(group, write);

        assertEquals(future, manager.write(group, write));

        verify(async, never()).resolved(any(WriteResult.class));
        verify(write).isEmpty();
        verify(manager).syncWrite(group, write);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDoWriteAll() throws BackendGroupException {
        final IngestionManagerImpl manager = setupIngestionManager(true, true, true);

        doReturn(future).when(manager).doMetricWrite(write, metric);
        doReturn(future).when(manager).doMetadataWrite(write, metadata, suggest);
        doReturn(future).when(async).collect(anyCollection(), Matchers.<Collector>any(Collector.class));

        assertEquals(future, manager.syncWrite(group, write));

        verify(future).onFinished(any());
        verify(reporter).incrementConcurrentWrites();
        verify(manager).doMetricWrite(write, metric);
        verify(manager).doMetadataWrite(write, metadata, suggest);
        verify(async).collect(anyCollection(), Matchers.<Collector> any(Collector.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDoWriteNone() throws BackendGroupException {
        final IngestionManagerImpl manager = setupIngestionManager(false, false, false);

        doReturn(future).when(manager).doMetricWrite(write, metric);
        doReturn(future).when(manager).doMetadataWrite(write, metadata, suggest);
        doReturn(future).when(async).collect(anyCollection(), Matchers.<Collector>any(Collector.class));

        assertEquals(future, manager.syncWrite(group, write));

        verify(future).onFinished(any());
        verify(reporter).incrementConcurrentWrites();
        verify(manager, never()).doMetricWrite(write, metric);
        verify(manager, never()).doMetadataWrite(write, metadata, suggest);
        verify(async).collect(anyCollection(), Matchers.<Collector> any(Collector.class));
    }
}

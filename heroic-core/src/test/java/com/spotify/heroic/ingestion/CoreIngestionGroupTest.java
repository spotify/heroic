package com.spotify.heroic.ingestion;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import io.opencensus.trace.BlankSpan;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CoreIngestionGroupTest {
    @Mock
    private AsyncFramework async;
    @Mock
    private Supplier<Filter> filterSupplier;
    @Mock
    private Supplier<DateRange> rangeSupplier;
    @Mock
    private Filter filter;
    @Mock
    private Semaphore writePermits;
    @Mock
    private IngestionManagerReporter reporter;
    @Mock
    private LongAdder ingested;
    @Mock
    private MetricBackend metric;
    @Mock
    private MetadataBackend metadata;
    @Mock
    private SuggestBackend suggest;
    @Mock
    private Request request;
    @Mock
    private AsyncFuture<Ingestion> expected;
    @Mock
    private AsyncFuture<Ingestion> other;
    @Mock
    private Series series;
    @Mock
    private DateRange range;

    @Before
    public void setup() {
        doReturn(series).when(request).getSeries();

        doAnswer(invocation -> {
            ((FutureFinished) invocation.getArguments()[0]).finished();
            return expected;
        }).when(expected).onFinished(any(FutureFinished.class));

        doReturn(range).when(rangeSupplier).get();
    }

    private CoreIngestionGroup setupIngestionGroup(
        final Optional<MetricBackend> metric,
        final Optional<MetadataBackend> metadata,
        final Optional<SuggestBackend> suggest
    ) {
        final CoreIngestionGroup group = new CoreIngestionGroup(
            async, filterSupplier, writePermits, reporter, ingested,
            metric, metadata, suggest
        );

        return spy(group);
    }

    @Test
    public void testWriteSome() {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(expected).when(group).syncWrite(request);

        assertEquals(expected, group.write(request));

        verify(async, never()).resolved(any(Ingestion.class));
        verify(ingested).increment();
        verify(group).syncWrite(request);
    }

    @Test
    public void testSyncWrite() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(filter).when(filterSupplier).get();
        doReturn(true).when(filter).apply(series);
        doNothing().when(writePermits).acquire();
        doNothing().when(writePermits).release();
        doReturn(expected).when(group).doWrite(eq(request), any());

        assertEquals(expected, group.syncWrite(request));

        verify(async, never()).resolved(any(Ingestion.class));
        verify(async, never()).failed(any(Throwable.class));
        verify(writePermits).acquire();
        verify(writePermits).release();
        verify(reporter).incrementConcurrentWrites();
        verify(reporter).decrementConcurrentWrites();
        verify(group).doWrite(eq(request), any());
        verify(expected).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testSyncWriteFiltered() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(filter).when(filterSupplier).get();
        doReturn(expected).when(async).resolved(any(Ingestion.class));
        doReturn(false).when(filter).apply(series);

        assertEquals(expected, group.syncWrite(request));

        verify(async).resolved(any(Ingestion.class));
        verify(async, never()).failed(any(Throwable.class));
        verify(writePermits, never()).acquire();
        verify(writePermits, never()).release();
        verify(reporter, never()).incrementConcurrentWrites();
        verify(reporter, never()).decrementConcurrentWrites();
        verify(reporter).reportDroppedByFilter();
        verify(group, never()).doWrite(eq(request), any());
        verify(other, never()).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testSyncWriteAcquireThrows() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        final InterruptedException e = new InterruptedException();

        doReturn(filter).when(filterSupplier).get();
        doReturn(expected).when(async).failed(any(Throwable.class));
        doReturn(true).when(filter).apply(series);
        doThrow(e).when(writePermits).acquire();

        assertEquals(expected, group.syncWrite(request));

        verify(async, never()).resolved(any(Ingestion.class));
        verify(async).failed(any(Throwable.class));
        verify(writePermits).acquire();
        verify(writePermits, never()).release();
        verify(reporter, never()).incrementConcurrentWrites();
        verify(reporter, never()).decrementConcurrentWrites();
        verify(group, never()).doWrite(eq(request), any());
        verify(other, never()).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testDoWrite() {
        final CoreIngestionGroup group = setupIngestionGroup(of(metric), of(metadata), of(suggest));

        final List<AsyncFuture<Ingestion>> futures = ImmutableList.of(other, other, other);

        doReturn(rangeSupplier).when(group).rangeSupplier(request);
        doReturn(expected).when(async).collect(futures, Ingestion.reduce());

        doReturn(other).when(group).doMetricWrite(eq(metric), eq(request), any());
        doReturn(other).when(group).doMetadataWrite(eq(metadata), eq(request), eq(range), any());
        doReturn(other).when(group).doSuggestWrite(eq(suggest), eq(request), eq(range), any());

        assertEquals(expected, group.doWrite(request, BlankSpan.INSTANCE));

        verify(group).rangeSupplier(request);
        verify(group).doMetricWrite(eq(metric), eq(request), any());
        verify(group).doMetadataWrite(eq(metadata), eq(request), eq(range), any());
        verify(group).doSuggestWrite(eq(suggest), eq(request), eq(range), any());
        verify(rangeSupplier, times(2)).get();
    }

    @Test
    public void testDoWriteSome() {
        final CoreIngestionGroup group = setupIngestionGroup(of(metric), empty(), of(suggest));

        final List<AsyncFuture<Ingestion>> futures = ImmutableList.of(other, other);

        doReturn(rangeSupplier).when(group).rangeSupplier(request);
        doReturn(expected).when(async).collect(futures, Ingestion.reduce());

        doReturn(other).when(group).doMetricWrite(eq(metric), eq(request), any());
        doReturn(other).when(group).doSuggestWrite(eq(suggest), eq(request), eq(range), any());

        assertEquals(expected, group.doWrite(request, BlankSpan.INSTANCE));

        verify(group).rangeSupplier(request);
        verify(group).doMetricWrite(eq(metric), eq(request), any());
        verify(group, never()).doMetadataWrite(eq(metadata), eq(request), eq(range), any());
        verify(group).doSuggestWrite(eq(suggest), eq(request), eq(range), any());
        verify(rangeSupplier, times(1)).get();
    }
}

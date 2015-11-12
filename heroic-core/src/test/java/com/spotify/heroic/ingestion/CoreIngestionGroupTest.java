package com.spotify.heroic.ingestion;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;

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
    private WriteMetric write;
    @Mock
    private AsyncFuture<WriteResult> expected;
    @Mock
    private AsyncFuture<WriteResult> other;
    @Mock
    private Series series;
    @Mock
    private DateRange range;

    @Before
    public void setup() {
        doReturn(series).when(write).getSeries();

        doAnswer(new Answer<AsyncFuture<WriteResult>>() {
            @Override
            public AsyncFuture<WriteResult> answer(final InvocationOnMock invocation)
                    throws Throwable {
                ((FutureFinished) invocation.getArguments()[0]).finished();
                return expected;
            }
        }).when(expected).onFinished(any(FutureFinished.class));

        doReturn(other).when(other).onFinished(any(FutureFinished.class));

        doReturn(range).when(rangeSupplier).get();
    }

    private CoreIngestionGroup setupIngestionGroup(final Optional<MetricBackend> metric,
            final Optional<MetadataBackend> metadata, final Optional<SuggestBackend> suggest) {
        // @formatter:off
        final CoreIngestionGroup group = new CoreIngestionGroup(
            async, filterSupplier, writePermits, reporter, ingested,
            metric, metadata, suggest
        );
        // @formatter:on

        return spy(group);
    }

    @Test
    public void testWriteEmpty() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(other).when(group).syncWrite(write);
        doReturn(true).when(write).isEmpty();
        doReturn(expected).when(async).resolved(any(WriteResult.class));

        assertEquals(expected, group.write(write));

        verify(async).resolved(any(WriteResult.class));
        verify(write).isEmpty();
        verify(ingested, never()).increment();
        verify(group, never()).syncWrite(write);
    }

    @Test
    public void testWriteSome() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(expected).when(group).syncWrite(write);
        doReturn(false).when(write).isEmpty();
        doReturn(other).when(async).resolved(any(WriteResult.class));

        assertEquals(expected, group.write(write));

        verify(async, never()).resolved(any(WriteResult.class));
        verify(write).isEmpty();
        verify(ingested).increment();
        verify(group).syncWrite(write);
    }

    @Test
    public void testSyncWrite() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(filter).when(filterSupplier).get();
        doReturn(other).when(async).failed(any(Throwable.class));
        doReturn(other).when(async).resolved(any(WriteResult.class));
        doReturn(true).when(filter).apply(series);
        doNothing().when(writePermits).acquire();
        doNothing().when(writePermits).release();
        doReturn(expected).when(group).doWrite(write);

        assertEquals(expected, group.syncWrite(write));

        verify(async, never()).resolved(any(WriteResult.class));
        verify(async, never()).failed(any(Throwable.class));
        verify(writePermits).acquire();
        verify(writePermits).release();
        verify(reporter).incrementConcurrentWrites();
        verify(reporter).decrementConcurrentWrites();
        verify(group).doWrite(write);
        verify(expected).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testSyncWriteFiltered() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        doReturn(filter).when(filterSupplier).get();
        doReturn(other).when(async).failed(any(Throwable.class));
        doReturn(expected).when(async).resolved(any(WriteResult.class));
        doReturn(false).when(filter).apply(series);
        doNothing().when(writePermits).acquire();
        doNothing().when(writePermits).release();

        doReturn(other).when(expected).onFinished(any(FutureFinished.class));
        doReturn(other).when(group).doWrite(write);

        assertEquals(expected, group.syncWrite(write));

        verify(async).resolved(any(WriteResult.class));
        verify(async, never()).failed(any(Throwable.class));
        verify(writePermits, never()).acquire();
        verify(writePermits, never()).release();
        verify(reporter, never()).incrementConcurrentWrites();
        verify(reporter, never()).decrementConcurrentWrites();
        verify(group, never()).doWrite(write);
        verify(other, never()).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testSyncWriteAcquireThrows() throws Exception {
        final CoreIngestionGroup group = setupIngestionGroup(empty(), empty(), empty());

        final InterruptedException e = new InterruptedException();

        doReturn(filter).when(filterSupplier).get();
        doReturn(expected).when(async).failed(any(Throwable.class));
        doReturn(other).when(async).resolved(any(WriteResult.class));
        doReturn(true).when(filter).apply(series);
        doThrow(e).when(writePermits).acquire();
        doNothing().when(writePermits).release();

        doReturn(other).when(expected).onFinished(any(FutureFinished.class));
        doReturn(other).when(group).doWrite(write);

        assertEquals(expected, group.syncWrite(write));

        verify(async, never()).resolved(any(WriteResult.class));
        verify(async).failed(any(Throwable.class));
        verify(writePermits).acquire();
        verify(writePermits, never()).release();
        verify(reporter, never()).incrementConcurrentWrites();
        verify(reporter, never()).decrementConcurrentWrites();
        verify(group, never()).doWrite(write);
        verify(other, never()).onFinished(any(FutureFinished.class));
    }

    @Test
    public void testDoWrite() {
        final CoreIngestionGroup group = setupIngestionGroup(of(metric), of(metadata), of(suggest));

        final List<AsyncFuture<WriteResult>> futures = ImmutableList.of(other, other, other);

        doReturn(rangeSupplier).when(group).rangeSupplier(write);
        doReturn(expected).when(async).collect(futures, WriteResult.merger());

        doReturn(other).when(group).doMetricWrite(metric, write);
        doReturn(other).when(group).doMetadataWrite(metadata, write, range);
        doReturn(other).when(group).doSuggestWrite(suggest, write, range);

        assertEquals(expected, group.doWrite(write));

        verify(group).rangeSupplier(write);
        verify(group).doMetricWrite(metric, write);
        verify(group).doMetadataWrite(metadata, write, range);
        verify(group).doSuggestWrite(suggest, write, range);
        verify(rangeSupplier, times(2)).get();
    }

    @Test
    public void testDoWriteSome() {
        final CoreIngestionGroup group = setupIngestionGroup(of(metric), empty(), of(suggest));

        final List<AsyncFuture<WriteResult>> futures = ImmutableList.of(other, other);

        doReturn(rangeSupplier).when(group).rangeSupplier(write);
        doReturn(expected).when(async).collect(futures, WriteResult.merger());

        doReturn(other).when(group).doMetricWrite(metric, write);
        doReturn(other).when(group).doMetadataWrite(metadata, write, range);
        doReturn(other).when(group).doSuggestWrite(suggest, write, range);

        assertEquals(expected, group.doWrite(write));

        verify(group).rangeSupplier(write);
        verify(group).doMetricWrite(metric, write);
        verify(group, never()).doMetadataWrite(metadata, write, range);
        verify(group).doSuggestWrite(suggest, write, range);
        verify(rangeSupplier, times(1)).get();
    }
}

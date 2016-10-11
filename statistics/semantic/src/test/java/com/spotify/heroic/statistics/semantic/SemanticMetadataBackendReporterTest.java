package com.spotify.heroic.statistics.semantic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindSeriesIdsStream;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.metrics.core.SemanticMetricRegistry;
import org.junit.Test;

public class SemanticMetadataBackendReporterTest {
    @Test
    public void testDecorated() {
        final SemanticMetricRegistry registry = mock(SemanticMetricRegistry.class);
        final SemanticMetadataBackendReporter reporter =
            new SemanticMetadataBackendReporter(registry);

        final MetadataBackend backend = mock(MetadataBackend.class);
        final MetadataBackend decorated = reporter.decorate(backend);

        // TODO: Currently covers the methods which have defender methods, but should cover all.

        {
            final AsyncObservable<FindSeriesIdsStream> response = mock(AsyncObservable.class);
            final FindSeriesIds.Request request = mock(FindSeriesIds.Request.class);
            doReturn(response).when(backend).findSeriesIdsStream(request);
            assertEquals(response, decorated.findSeriesIdsStream(request));
            verify(backend).findSeriesIdsStream(request);
        }

        {
            final AsyncObservable<FindSeriesStream> response = mock(AsyncObservable.class);
            final FindSeries.Request request = mock(FindSeries.Request.class);
            doReturn(response).when(backend).findSeriesStream(request);
            assertEquals(response, decorated.findSeriesStream(request));
            verify(backend).findSeriesStream(request);
        }

        {
            final Statistics statistics = mock(Statistics.class);
            doReturn(statistics).when(backend).getStatistics();
            assertEquals(statistics, decorated.getStatistics());
            verify(backend).getStatistics();
        }
    }
}

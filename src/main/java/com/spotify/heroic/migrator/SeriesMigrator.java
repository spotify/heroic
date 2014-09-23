package com.spotify.heroic.migrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.LocalMetadataManager;
import com.spotify.heroic.metrics.BackendGroup;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

public class SeriesMigrator {
    @Inject
    private ExecutorService executor;

    @Inject
    private LocalMetadataManager metadata;

    @Slf4j
    @RequiredArgsConstructor
    public static class MigratorThread implements Runnable {
        private final String session;
        private final AtomicInteger count;
        private final int total;
        private final BackendGroup source;
        private final BackendGroup target;
        private final DateRange range;
        private final Series series;
        private final Semaphore available;

        @Override
        public void run() {
            final int id = count.incrementAndGet();

            final String threadSession = String.format("%s-%04d/%04d", session,
                    id, total);

            try {
                executeOne(threadSession, source, target, range);
            } catch (final Exception e) {
                log.error(String.format("%s: Migrate of %s failed", session,
                        series), e);
            }

            available.release();
        }

        private void executeOne(final String session,
                final BackendGroup source, final BackendGroup target,
                final DateRange range) throws Exception {
            final Callback.Reducer<FetchData, List<DataPoint>> reducer = new Callback.Reducer<FetchData, List<DataPoint>>() {
                @Override
                public List<DataPoint> resolved(Collection<FetchData> results,
                        Collection<Exception> errors,
                        Collection<CancelReason> cancelled) throws Exception {
                    for (final Exception e : errors)
                        log.error("{}: Failed to read entry", session, e);

                    for (final CancelReason reason : cancelled)
                        log.error("{}, Entry read cancelled: {}", session,
                                reason);

                    if (errors.size() > 0 || cancelled.size() > 0)
                        throw new Exception("Errors during read");

                    final List<DataPoint> datapoints = new ArrayList<>();

                    for (final FetchData r : results)
                        datapoints.addAll(r.getDatapoints());

                    Collections.sort(datapoints);

                    return datapoints;
                }
            };

            final List<DataPoint> datapoints = ConcurrentCallback.newReduce(
                    source.query(series, range), reducer).get();

            log.info(String.format(
                    "%s: Writing %d datapoint(s) for series: %s", session,
                    datapoints.size(), series));

            target.write(
                    Arrays.asList(new WriteMetric[] { new WriteMetric(series,
                            datapoints) })).get();
        }
    };

    public void migrate(long history, final BackendGroup source,
            final BackendGroup target, final Filter filter) throws Exception {
        final Set<Series> series;

        try {
            series = metadata.findSeries(filter).get().getSeries();
        } catch (final Exception e) {
            throw new Exception("Failed to load time series to migrate", e);
        }

        final long now = System.currentTimeMillis();

        final DateRange range = new DateRange(now - history, now);

        final String session = Integer.toHexString(new Object().hashCode());
        final Semaphore available = new Semaphore(50, true);
        final AtomicInteger count = new AtomicInteger(0);

        final int total = series.size();

        for (final Series s : series) {
            available.acquire();

            final Runnable runnable = new MigratorThread(session, count, total,
                    source, target, range, s, available);

            executor.execute(runnable);
        }
    }
}

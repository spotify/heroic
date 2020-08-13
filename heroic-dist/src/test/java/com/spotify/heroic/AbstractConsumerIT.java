package com.spotify.heroic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchData.Request;
import com.spotify.heroic.metric.FetchData.Result;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricReadResult;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ClockSource;
import eu.toolchain.async.RetryPolicy;
import io.opencensus.trace.BlankSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractConsumerIT extends AbstractSingleNodeIT {
    private static final RetryPolicy RETRY_POLICY =
        RetryPolicy.timed(1000, RetryPolicy.exponential(10, 100));

    protected boolean expectAtLeastOneCommit = false;

    protected Consumer<WriteMetric.Request> consumer;

    protected abstract Consumer<WriteMetric.Request> setupConsumer();

    @Before
    public void basicSetup() {
        this.consumer = setupConsumer();
        this.expectAtLeastOneCommit = false;
    }

    @Test
    public void consumeOneMessage() throws Exception {
        final Series s1 = Series.of("s1", ImmutableMap.of("host", "localhost"));
        final MetricCollection mc =
            MetricCollection.points(ImmutableList.of(new Point(10, 42), new Point(20, 43)));
        WriteMetric.Request request = new WriteMetric.Request(s1, mc);

        consumer.accept(request);

        tryUntil(() -> {
            final List<MetricReadResult> data = Collections.synchronizedList(new ArrayList<>());

            final var resultAsync = instance.inject(coreComponent -> {
                Request fetchDataRequest =
                    new Request(MetricType.POINT, s1, new DateRange(0, 100),
                        QueryOptions.defaults());

                final AsyncFuture<Result> fetchAsync = coreComponent
                    .metricManager()
                    .useDefaultGroup()
                    .fetch(fetchDataRequest,
                        FetchQuotaWatcher.NO_QUOTA,
                        data::add,
                        BlankSpan.INSTANCE
                    );
                return fetchAsync;
            });

            // OK, let's actually make/wait-for the calls.
            resultAsync.get();

            assertFalse(data.isEmpty());
            final MetricCollection collection = data.iterator().next().getMetrics();
            assertEquals(mc, collection);
            return null;
        });
    }

    @Test
    public void consumeManyMessages() throws Exception {
        expectAtLeastOneCommit = true;
        final Series s1 = Series.of("s1", ImmutableMap.of("host", "localhost"));

        List<Point> consumedPoints = new ArrayList<>();
        for (int count = 1; count <= 10; count++) {
            Point p = new Point(count, 42);
            final MetricCollection mc = MetricCollection.points(ImmutableList.of(p));
            consumedPoints.add(p);

            WriteMetric.Request request = new WriteMetric.Request(s1, mc);

            consumer.accept(request);
            Thread.sleep(100);
        }

        tryUntil(() -> {
            final List<MetricReadResult> data = Collections.synchronizedList(new ArrayList<>());

            instance.inject(coreComponent -> {
                FetchData.Request fetchDataRequest =
                    new FetchData.Request(MetricType.POINT, s1, new DateRange(0, 100),
                        QueryOptions.defaults());
                return coreComponent
                    .metricManager()
                    .useDefaultGroup()
                    .fetch(fetchDataRequest,
                        FetchQuotaWatcher.NO_QUOTA,
                        data::add,
                        BlankSpan.INSTANCE
                    );
            }).get();

            assertFalse(data.isEmpty());
            final MetricCollection metricCollection = data.iterator().next().getMetrics();
            List<Metric> collection = new ArrayList<>(metricCollection.data());
            collection.sort(Metric.comparator);
            assertEquals(consumedPoints, collection);
            return null;
        });
    }

    private void tryUntil(Callable<Void> callable) throws Exception {
        RetryPolicy.Instance instance = RETRY_POLICY.apply(ClockSource.SYSTEM);
        List<Throwable> supressed = new ArrayList<>();

        while (true) {
            try {
                callable.call();
            } catch (AssertionError e) {
                RetryPolicy.Decision decision = instance.next();
                if (!decision.shouldRetry()) {
                    supressed.forEach(e::addSuppressed);
                    throw e;
                }

                supressed.add(e);
                Thread.sleep(decision.backoff());
                continue;
            }

            break;
        }
    }
}

package com.spotify.heroic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.schemas.spotify100.Version;
import com.spotify.heroic.consumer.schemas.spotify100.v2.Value;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.HeroicDistribution;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricReadResult;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
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
    protected Consumer<WriteMetric.Request> consumerV2;

    protected abstract Consumer<WriteMetric.Request> setupConsumer(Version version);

    @Before
    public void basicSetup() {
        this.consumer = setupConsumer(new Version(1,0,1));
        this.consumerV2 = setupConsumer(new Version(2,0,0));
        this.expectAtLeastOneCommit = false;
    }

    @Test
    public void consumeOneV2Message() throws Exception {
        final Series s1 = Series.of("s1", ImmutableMap.of("host", "localhost"));

        HeroicDistribution distribution1 = HeroicDistributionGenerator.generateOne();
        DistributionPoint point1 = DistributionPoint.create(distribution1,10);

        HeroicDistribution distribution2 = HeroicDistributionGenerator.generateOne();
        DistributionPoint point2 = DistributionPoint.create(distribution2,20);

        final MetricCollection mc =
            MetricCollection.distributionPoints(List.of(point1, point2));
        WriteMetric.Request request = new WriteMetric.Request(s1, mc);

        consumerV2.accept(request);

        tryUntil(() -> {
            final List<MetricReadResult> data = Collections.synchronizedList(new ArrayList<>());

            instance.inject(coreComponent -> {
                FetchData.Request fetchDataRequest =
                    new FetchData.Request(MetricType.DISTRIBUTION_POINTS, s1, new DateRange(0, 100),
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
            final MetricCollection collection = data.iterator().next().getMetrics();
            assertEquals(mc, collection);
            return null;
        });
    }

    @Test
    public void consumeOneMessage() throws Exception {
        final Series s1 = Series.of("s1", ImmutableMap.of("host", "localhost"));
        final MetricCollection mc =
            MetricCollection.points(ImmutableList.of
                (new Point(10, 42), new Point(20, 43)));
        WriteMetric.Request request = new WriteMetric.Request(s1, mc);

        consumer.accept(request);

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
            final MetricCollection collection = data.iterator().next().getMetrics();
            assertEquals(mc, collection);
            return null;
        });
    }

    @Test
    public void consumeManyV2Messages() throws Exception {
        expectAtLeastOneCommit = true;
        final Series s1 = Series.of("s1", ImmutableMap.of("host", "localhost"));


        int count = 10;
        List<DistributionPoint> consumedPoints = new ArrayList<>();
        List<HeroicDistribution> distriubtions = HeroicDistributionGenerator.GenerateMany(count);
        long timestamp = 0L;
        for (HeroicDistribution distribution : distriubtions) {

            timestamp +=10;

            DistributionPoint point = DistributionPoint.create(distribution,timestamp);

            final MetricCollection mc =
                MetricCollection.distributionPoints(ImmutableList.of(point));

            consumedPoints.add(point);

            WriteMetric.Request request = new WriteMetric.Request(s1, mc);

            consumerV2.accept(request);
            Thread.sleep(100);
        }

        tryUntil(() -> {
            final List<MetricReadResult> data = Collections.synchronizedList(new ArrayList<>());

            instance.inject(coreComponent -> {
                FetchData.Request fetchDataRequest =
                    new FetchData.Request(MetricType.DISTRIBUTION_POINTS, s1, new DateRange(0, 100),
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

    static List<TMetric> createTestMetricCollection(final WriteMetric.Request request ) {

        List<TMetric> metrics = new ArrayList<>();
        int n = -1;
        while (++n < request.getData().size()) {
            Value val = null ;
            long timestamp = 0;
            if (request.getData().getType().equals(MetricType.POINT)) {
                final Point p =  request.getData().getDataAs(Point.class).get(n);
                val = Value.DoubleValue.create(p.getValue());
                timestamp = p.getTimestamp();

            }else if (request.getData().getType().equals(MetricType.DISTRIBUTION_POINTS)) {
                final DistributionPoint p = request.getData()
                    .getDataAs(DistributionPoint.class).get(n);
                val = Value.DistributionValue.create(p.value().getValue());
                timestamp = p.getTimestamp();
            }
            TMetric metric = new TMetric(timestamp, val);
            metrics.add(metric);
        }
        return metrics;
    }


    Object createJsonMetric(final TMetric metric, final Series series){
        Object jsonMetric;
        if (metric.getValue() instanceof  Value.DoubleValue){
            jsonMetric =
                new DataVersion1("1.1.0", series.getKey(), "localhost",metric.getTimestamp(),
                    series.getTags(), series.getResource(), (Double)metric.getValue().getValue());
        }else if ( metric.getValue() instanceof  Value.DistributionValue){
            jsonMetric =
                new DataVersion2("2.0.0", series.getKey(), "localhost",metric.getTimestamp(),
                    series.getTags(), series.getResource(), metric.getValue());
        }else {
            throw new RuntimeException("Unsupported type");
        }
        return jsonMetric;
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

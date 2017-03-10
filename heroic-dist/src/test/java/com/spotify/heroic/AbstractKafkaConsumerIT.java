package com.spotify.heroic;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.kafka.FakeKafkaConnection;
import com.spotify.heroic.consumer.kafka.KafkaConsumerModule;
import com.spotify.heroic.consumer.schemas.Spotify100;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.instrumentation.OperationsLogImpl;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.memory.MemoryMetricModule;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.After;

public abstract class AbstractKafkaConsumerIT extends AbstractConsumerIT {
    // Use commit interval 0 to get a commit after each message being written
    private static final int COMMIT_INTERVAL = 50;

    protected final String topic = "topic1";
    private final ObjectMapper objectMapper = new ObjectMapper();

    protected OperationsLogImpl opLog;

    private FakeKafkaConnection connection;

    abstract boolean useTransactionalConsumer();

    @Override
    protected HeroicConfig.Builder setupConfig() {
        opLog = new OperationsLogImpl();

        connection = new FakeKafkaConnection(opLog);

        final MetricModule backingStore = MemoryMetricModule.builder().build();

        MetricModule metricModule = new LoggingMetricModule(backingStore, opLog);
        return HeroicConfig
            .builder()
            .stopTimeout(Duration.of(5, TimeUnit.SECONDS))
            .consumers(ImmutableList.of(KafkaConsumerModule
                .builder()
                .topics(ImmutableList.of(topic))
                .schema(Spotify100.class)
                .fakeKafkaConnection(connection)
                .transactional(useTransactionalConsumer())
                .transactionCommitInterval(COMMIT_INTERVAL)))
            .ingestion(IngestionModule.builder().updateMetrics(true))
            .metrics(MetricManagerModule
                .builder()
                .backends(ImmutableList.<MetricModule>of(metricModule)));
    }

    @Override
    protected Consumer<WriteMetric.Request> setupConsumer() {
        return request -> {
            final MetricCollection mc = request.getData();

            if (mc.getType() != MetricType.POINT) {
                throw new RuntimeException("Unsupported metric type: " + mc.getType());
            }

            final Series series = request.getSeries();
            for (final Point p : mc.getDataAs(Point.class)) {
                final Spotify100.JsonMetric src =
                    new Spotify100.JsonMetric(Spotify100.SCHEMA_VERSION, series.getKey(),
                        "localhost", p.getTimestamp(), series.getTags(), p.getValue());

                final byte[] message;
                try {
                    message = objectMapper.writeValueAsBytes(src);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                connection.publish(topic, message);
            }
        };
    }

    @After
    public void verifyTransactionality() {
        if (!useTransactionalConsumer()) {
            return;
        }

        long writeRequests = 0;
        long writeCompletions = 0;
        long offsetsCommits = 0;

        for (OperationsLogImpl.OpType op : opLog.getLog()) {
            if (op == OperationsLogImpl.OpType.WRITE_REQUEST) {
                writeRequests++;
            }

            if (op == OperationsLogImpl.OpType.WRITE_COMPLETE) {
                writeCompletions++;
            }

            if (op == OperationsLogImpl.OpType.OFFSETS_COMMIT) {
                offsetsCommits++;
                assertEquals(
                    "all write requests should have completed, at the point of offsets commit",
                    writeRequests, writeCompletions);
            }
        }

        if (expectAtLeastOneCommit) {
            assertTrue(offsetsCommits > 0);
        }
    }
}

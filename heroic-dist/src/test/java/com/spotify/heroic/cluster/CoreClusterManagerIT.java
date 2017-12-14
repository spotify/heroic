package com.spotify.heroic.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.AbstractLocalClusterIT;
import com.spotify.heroic.HeroicCoreInstance;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class CoreClusterManagerIT extends AbstractLocalClusterIT {
    private final ControlledNodeMetadataFactory metadata1 = new ControlledNodeMetadataFactory();
    private final ControlledNodeMetadataFactory metadata2 = new ControlledNodeMetadataFactory();

    private final List<ControlledNodeMetadataFactory> metadataFactories =
        ImmutableList.of(metadata1, metadata2);

    private static final Random random = new Random();

    @Override
    protected String protocol() {
        return "grpc";
    }

    @Override
    protected List<NodeMetadataFactory> metadataFactories() {
        return ImmutableList.of(metadata1, metadata2);
    }

    /**
     * This tests the assumption that it is safe to perform refreshes, even when there are
     * requests pending, or about to be fired.
     *
     * The test will continue to run, while the refresh thread is running.
     */
    @Test
    public void testSafeRefresh() throws Exception {
        final int numberOfRefreshes = 100;
        final int requestsPerIteration = 100;

        final HeroicCoreInstance a = instances.get(0);
        final ClusterManager clusterManager = a.inject(ClusterComponent::clusterManager);

        /* setup a thread that performs the given number of refresh iterations */
        final ClusterRefreshThread t = setupRefreshThread(clusterManager, numberOfRefreshes);

        final DataStreamCollector collector = new DataStreamCollector();

        while (!t.shutdown.get()) {
            final List<Callable<AsyncFuture<Void>>> operations = new ArrayList<>();

            for (int i = 0; i < requestsPerIteration; i++) {
                operations.add(() -> {
                    final List<AsyncFuture<Void>> pongs = new ArrayList<>();

                    for (final ClusterShard shard : clusterManager.useDefaultGroup()) {
                        for (int p = 0; p < 10; p++) {
                            pongs.addAll(pingAllNodesInShard(clusterManager, shard));
                        }
                    }

                    return async.collectAndDiscard(pongs);
                });
            }

            async.eventuallyCollect(operations, collector, 20).get();
        }

        t.join();

        assertNull("no errors during refreshes", t.refreshError.get());
        assertTrue("number of refreshes are non-zero", t.refreshes.get() > 0);
        assertTrue("number of resolved requests are non-zero", collector.resolved.get() > 0);
        assertEquals("expect no cancelled requests", 0, collector.cancelled.get());
        assertEquals("expect no failed requests", 0, collector.failed.get());
        assertTrue(collector.ended);
    }

    private List<AsyncFuture<Void>> pingAllNodesInShard(
        ClusterManager clusterManager, ClusterShard shard
    ) {
        final List<AsyncFuture<Void>> futures = Lists.newArrayList();
        final List<ClusterNode> excludeIds = Lists.newArrayList();
        while (true) {
            Optional<ClusterManager.NodeResult<AsyncFuture<Void>>> ret =
                clusterManager.withNodeInShardButNotWithId(shard.getShard(), excludeIds::contains,
                    excludeIds::add, ClusterNode.Group::ping);
            if (!ret.isPresent()) {
                // No more nodes available in shard, we're done
                return futures;
            }
            ClusterManager.NodeResult<AsyncFuture<Void>> result = ret.get();
            futures.add(result.getReturnValue());
        }
    }

    private ClusterRefreshThread setupRefreshThread(
        final ClusterManager clusterManager, final int iterations
    ) {
        final ClusterRefreshThread t = new ClusterRefreshThread(clusterManager, iterations);
        t.setName("refresh-test-" + UUID.randomUUID());
        t.start();
        return t;
    }

    @RequiredArgsConstructor
    private class ClusterRefreshThread extends Thread {
        private final ClusterManager clusterManager;
        private final int iterations;

        private final AtomicReference<Exception> refreshError = new AtomicReference<>();
        private final AtomicInteger refreshes = new AtomicInteger();
        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        @Override
        public void run() {
            for (int i = 0; i < iterations; i++) {
                /* randomize metadata state to cause refreshes/failures */
                for (final ControlledNodeMetadataFactory factory : metadataFactories) {
                    switch (random.nextInt(2)) {
                        case 0:
                            factory.setId(UUID.randomUUID());
                            break;
                        case 1:
                            factory.setFail(!factory.fail);
                            break;
                    }
                }

                try {
                    clusterManager.refresh().get();
                } catch (final Exception e) {
                    refreshError.set(e);
                    break;
                }

                refreshes.addAndGet(1);
            }

            shutdown.set(true);
        }
    }

    private static class DataStreamCollector implements StreamCollector<Void, Void> {
        private final AtomicInteger resolved = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicInteger cancelled = new AtomicInteger();
        private boolean ended = false;

        @Override
        public void resolved(final Void result) throws Exception {
            resolved.getAndAdd(1);
        }

        @Override
        public void failed(final Throwable cause) throws Exception {
            failed.getAndAdd(1);
        }

        @Override
        public void cancelled() throws Exception {
            cancelled.getAndAdd(1);
        }

        @Override
        public Void end(final int resolved, final int failed, final int cancelled)
            throws Exception {
            ended = true;
            return null;
        }
    }

    private static class ControlledNodeMetadataFactory implements NodeMetadataFactory {
        private Optional<UUID> id = Optional.empty();
        private boolean fail = false;

        public void setId(UUID id) {
            this.id = Optional.of(id);
        }

        public void setFail(boolean fail) {
            this.fail = fail;
        }

        @Override
        public NodeMetadataProvider buildProvider(
            final NodeMetadata localMetadata
        ) {
            return () -> {
                if (fail) {
                    throw new RuntimeException("a failure");
                }

                NodeMetadata node = localMetadata;

                if (id.isPresent()) {
                    node = new NodeMetadata(node.getVersion(), id.get(), node.getTags(),
                        node.getService());
                }

                return node;
            };
        }
    }
}

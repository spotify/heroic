package com.spotify.heroic;

import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.instrumentation.OperationsLog;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.function.Consumer;
import lombok.Data;

@Data
class LoggingMetricModule implements MetricModule {
    private final MetricModule backingStore;
    private final OperationsLog opLog;

    @Override
    public Exposed module(
        final PrimaryComponent primary, final Depends backend, final String id
    ) {
        final Exposed backingStoreModule = backingStore.module(primary, backend, id);

        return new Exposed() {

            @Override
            public LifeCycle life() {
                return backingStoreModule.life();
            }

            @Override
            public MetricBackend backend() {
                final MetricBackend delegate = backingStoreModule.backend();

                return new LoggingMetricBackend(delegate);
            }
        };
    }

    @Data
    private class LoggingMetricBackend implements MetricBackend {
        private final MetricBackend delegate;

        @Override
        public Statistics getStatistics() {
            return delegate.getStatistics();
        }

        @Override
        public AsyncFuture<Void> configure() {
            return delegate.configure();
        }

        @Override
        public AsyncFuture<WriteMetric> write(
            final WriteMetric.Request write
        ) {
            opLog.registerWriteRequest();
            return delegate.write(write).directTransform(result -> {
                opLog.registerWriteComplete();
                return result;
            });
        }

        @Override
        public AsyncFuture<FetchData> fetch(
            final FetchData.Request request, final FetchQuotaWatcher watcher
        ) {
            return delegate.fetch(request, watcher);
        }

        @Override
        public AsyncFuture<FetchData.Result> fetch(
            final FetchData.Request request, final FetchQuotaWatcher watcher,
            final Consumer<MetricCollection> metricsConsumer
        ) {
            return delegate.fetch(request, watcher, metricsConsumer);
        }

        @Override
        public Iterable<BackendEntry> listEntries() {
            return delegate.listEntries();
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeys(
            final BackendKeyFilter filter, final QueryOptions options
        ) {
            return delegate.streamKeys(filter, options);
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeysPaged(
            final BackendKeyFilter filter, final QueryOptions options, final long pageSize
        ) {
            return delegate.streamKeysPaged(filter, options, pageSize);
        }

        @Override
        public AsyncFuture<List<String>> serializeKeyToHex(
            final BackendKey key
        ) {
            return delegate.serializeKeyToHex(key);
        }

        @Override
        public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(
            final String key
        ) {
            return delegate.deserializeKeyFromHex(key);
        }

        @Override
        public AsyncFuture<Void> deleteKey(
            final BackendKey key, final QueryOptions options
        ) {
            return delegate.deleteKey(key, options);
        }

        @Override
        public AsyncFuture<Long> countKey(
            final BackendKey key, final QueryOptions options
        ) {
            return delegate.countKey(key, options);
        }

        @Override
        public AsyncFuture<MetricCollection> fetchRow(
            final BackendKey key
        ) {
            return delegate.fetchRow(key);
        }

        @Override
        public AsyncObservable<MetricCollection> streamRow(
            final BackendKey key
        ) {
            return delegate.streamRow(key);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Groups groups() {
            return delegate.groups();
        }
    }
}

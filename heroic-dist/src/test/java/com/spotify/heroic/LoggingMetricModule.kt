package com.spotify.heroic

import com.spotify.heroic.async.AsyncObservable
import com.spotify.heroic.common.Groups
import com.spotify.heroic.common.Statistics
import com.spotify.heroic.dagger.PrimaryComponent
import com.spotify.heroic.instrumentation.OperationsLog
import com.spotify.heroic.lifecycle.LifeCycle
import com.spotify.heroic.metric.*
import eu.toolchain.async.AsyncFuture
import io.opencensus.trace.Span
import java.util.function.Consumer

data class LoggingMetricModule(
    val backingStore: MetricModule, val opLog: OperationsLog
) : MetricModule {

    override fun module(
        primary: PrimaryComponent, backend: MetricModule.Depends, id: String
    ): MetricModule.Exposed {
        val backingStoreModule: MetricModule.Exposed = backingStore.module(primary, backend, id)

        return object : MetricModule.Exposed {
            override fun life(): LifeCycle {
                return backingStoreModule.life()
            }

            override fun backend(): MetricBackend {
                val delegate: MetricBackend = backingStoreModule.backend()

                return LoggingMetricBackend(delegate)
            }
        }
    }

    private inner class LoggingMetricBackend(val delegate: MetricBackend) : MetricBackend {

        override fun getStatistics(): Statistics {
            return delegate.statistics
        }

        override fun configure(): AsyncFuture<Void> {
            return delegate.configure()
        }

        override fun write(write: WriteMetric.Request): AsyncFuture<WriteMetric> {
            opLog.registerWriteRequest()
            return delegate.write(write).directTransform { result ->
                opLog.registerWriteComplete()
                result
            }
        }

        override fun fetch(
            request: FetchData.Request,
            watcher: FetchQuotaWatcher,
            metricsConsumer: Consumer<MetricReadResult>,
            span: Span
        ): AsyncFuture<FetchData.Result> {
            return delegate.fetch(request, watcher, metricsConsumer, span)
        }

        override fun listEntries(): Iterable<BackendEntry> {
            return delegate.listEntries()
        }

        override fun streamKeys(
            filter: BackendKeyFilter, options: QueryOptions
        ): AsyncObservable<BackendKeySet> {
            return delegate.streamKeys(filter, options)
        }

        override fun streamKeysPaged(
            filter: BackendKeyFilter, options: QueryOptions, pageSize: Long
        ): AsyncObservable<BackendKeySet> {
            return delegate.streamKeysPaged(filter, options, pageSize)
        }

        override fun serializeKeyToHex(key: BackendKey): AsyncFuture<List<String>> {
            return delegate.serializeKeyToHex(key)
        }

        override fun deserializeKeyFromHex(key: String): AsyncFuture<List<BackendKey>> {
            return delegate.deserializeKeyFromHex(key)
        }

        override fun deleteKey(key: BackendKey, options: QueryOptions): AsyncFuture<Void> {
            return delegate.deleteKey(key, options)
        }

        override fun countKey(key: BackendKey, options: QueryOptions): AsyncFuture<Long> {
            return delegate.countKey(key, options)
        }

        override fun fetchRow(key: BackendKey): AsyncFuture<MetricCollection> {
            return delegate.fetchRow(key)
        }

        override fun streamRow(key: BackendKey): AsyncObservable<MetricCollection> {
            return delegate.streamRow(key)
        }

        override fun isReady(): Boolean {
            return delegate.isReady
        }

        override fun groups(): Groups {
            return delegate.groups()
        }
    }
}

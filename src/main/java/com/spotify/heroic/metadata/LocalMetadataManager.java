package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@Slf4j
public class LocalMetadataManager {
    @Data
    public static class Config {
        private final List<MetadataBackend.Config> backends;

        @JsonCreator
        public static Config create(
                @JsonProperty("backends") List<MetadataBackend.Config> backends) {
            return new Config(backends);
        }

        public Module module(final MetadataBackendManagerReporter reporter) {
            return new PrivateModule() {
                @Override
                protected void configure() {
                    bindBackends(Config.this.backends);
                    bind(Config.class).toInstance(Config.this);
                    bind(MetadataBackendManagerReporter.class).toInstance(
                            reporter);

                    bind(LocalMetadataManager.class).in(Scopes.SINGLETON);
                    expose(LocalMetadataManager.class);
                }

                private void bindBackends(
                        final Collection<MetadataBackend.Config> configs) {
                    final Multibinder<MetadataBackend> bindings = Multibinder
                            .newSetBinder(binder(), MetadataBackend.class);

                    int i = 0;

                    for (final MetadataBackend.Config config : configs) {
                        final String id = Integer.toString(i++);
                        final Key<MetadataBackend> key = Key.get(
                                MetadataBackend.class, Names.named(id));

                        install(config.module(reporter.newMetadataBackend(id),
                                key));

                        bindings.addBinding().to(key);
                    }
                }

            };
        }
    }

    private final List<MetadataBackend> backends;
    private final MetadataBackendManagerReporter reporter;

    @Inject
    public LocalMetadataManager(final MetadataBackendManagerReporter reporter,
            Set<MetadataBackend> backends) {
        this.reporter = reporter;
        this.backends = new ArrayList<>(backends);
    }

    public List<MetadataBackend> getBackends() {
        return ImmutableList.copyOf(backends);
    }

    public Callback<FindTags> findTags(final Filter filter) {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindTags.reduce())
                .register(reporter.reportFindTags());
    }

    public Callback<String> writeSeries(Series series) {
        final String id = MetadataUtils.buildId(series);

        for (final MetadataBackend backend : backends) {
            try {
                backend.write(id, series);
            } catch (final MetadataOperationException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return new ResolvedCallback<>(id);
    }

    public Callback<FindSeries> findSeries(final Filter filter) {
        final List<Callback<FindSeries>> callbacks = new ArrayList<Callback<FindSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindSeries.reduce())
                .register(reporter.reportFindTimeSeries());
    }

    public Callback<DeleteSeries> deleteSeries(final Filter filter) {
        final List<Callback<DeleteSeries>> callbacks = new ArrayList<Callback<DeleteSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.deleteSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, DeleteSeries.reduce());
    }

    public Callback<FindKeys> findKeys(final Filter filter) {
        final List<Callback<FindKeys>> callbacks = new ArrayList<Callback<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindKeys.reduce())
                .register(reporter.reportFindKeys());
    }

    public Callback<Boolean> refresh() {
        final List<Callback<Void>> callbacks = new ArrayList<Callback<Void>>();

        for (final MetadataBackend backend : backends) {
            callbacks.add(backend.refresh());
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.DefaultStreamReducer<Void, Boolean>() {
            @Override
            public Boolean resolved(int successful, int failed,
                    int cancelled) throws Exception {
                return failed == 0 && cancelled == 0;
            }
        }).register(reporter.reportRefresh());
    }

    public boolean isReady() {
        boolean ready = true;

        for (final MetadataBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }
}

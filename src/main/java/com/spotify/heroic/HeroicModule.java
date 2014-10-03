package com.spotify.heroic;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.http.query.QueryResource.StoredMetricQueries;
import com.spotify.heroic.injection.CollectingTypeListener;
import com.spotify.heroic.injection.IsSubclassOf;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.ClusteredMetadataManager;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
public class HeroicModule extends AbstractModule {
    private final HeroicLifeCycle lifecycle;
    private final ScheduledExecutorService scheduledExecutor;
    private final Set<LifeCycle> lifecycles;
    private final HeroicReporter reporter;
    private final ObjectMapper mapper;

    @Provides
    @Singleton
    public HeroicReporter reporter() {
        return reporter;
    }

    @Override
    protected void configure() {
        bind(HeroicLifeCycle.class).toInstance(lifecycle);
        bind(ScheduledExecutorService.class).toInstance(scheduledExecutor);
        bind(ClusteredMetadataManager.class).in(Scopes.SINGLETON);
        bind(StoredMetricQueries.class).in(Scopes.SINGLETON);
        bind(ObjectMapper.class).toInstance(mapper);

        bindListener(new IsSubclassOf(LifeCycle.class), new CollectingTypeListener<LifeCycle>(lifecycles));
    }
};
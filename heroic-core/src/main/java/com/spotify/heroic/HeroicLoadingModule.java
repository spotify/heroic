package com.spotify.heroic;

import java.util.concurrent.ExecutorService;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.aggregationcache.AggregationCacheBackendModule;
import com.spotify.heroic.aggregationcache.CacheKey;
import com.spotify.heroic.aggregationcache.CacheKey_Serializer;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.common.CoreJavaxRestFramework;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.DurationSerialization;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.GroupsSerialization;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Series_Serializer;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.filter.CoreFilterFactory;
import com.spotify.heroic.filter.CoreFilterModifier;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.filter.FilterJsonDeserializer;
import com.spotify.heroic.filter.FilterJsonDeserializerImpl;
import com.spotify.heroic.filter.FilterJsonSerializer;
import com.spotify.heroic.filter.FilterJsonSerializerImpl;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.filter.FilterSerializer;
import com.spotify.heroic.filter.FilterSerializerImpl;
import com.spotify.heroic.grammar.CoreQueryParser;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.scheduler.DefaultScheduler;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.suggest.SuggestModule;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicLoadingModule extends AbstractModule {
    private final ExecutorService executor;
    private final HeroicInternalLifeCycle lifeCycle;
    private final HeroicOptions options;

    @Provides
    @Singleton
    HeroicOptions options() {
        return options;
    }

    @Provides
    @Singleton
    @Named("common")
    SerializerFramework serializer() {
        return TinySerializer.builder().build();
    }

    @Provides
    @Singleton
    FilterSerializer filterSerializer(@Named("common") SerializerFramework s) {
        return new FilterSerializerImpl(s, s.integer(), s.string());
    }

    @Provides
    @Singleton
    CoreAggregationRegistry aggregationRegistry(@Named("common") SerializerFramework s) {
        return new CoreAggregationRegistry(s.string());
    }

    @Provides
    @Singleton
    AggregationSerializer aggregationSerializer(CoreAggregationRegistry registry) {
        return registry;
    }

    @Provides
    @Singleton
    AggregationFactory aggregationFactory(CoreAggregationRegistry registry) {
        return registry;
    }

    @Provides
    @Singleton
    Serializer<CacheKey> cacheKeySerializer(@Named("common") SerializerFramework s, FilterSerializer filter,
            AggregationSerializer aggregation) {
        return new CacheKey_Serializer(s, filter, aggregation);
    }

    @Provides
    @Singleton
    Serializer<Series> series(@Named("common") SerializerFramework s) {
        return new Series_Serializer(s);
    }

    @Provides
    @Singleton
    public AsyncFramework async(ExecutorService executor) {
        return TinyAsync.builder().executor(executor).build();
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_HEROIC_CONFIG)
    private ObjectMapper configMapper() {
        final ObjectMapper m = new ObjectMapper(new YAMLFactory());

        m.addMixIn(AggregationCacheBackendModule.class, TypeNameMixin.class);
        m.addMixIn(ClusterDiscoveryModule.class, TypeNameMixin.class);
        m.addMixIn(RpcProtocolModule.class, TypeNameMixin.class);
        m.addMixIn(ConsumerModule.Builder.class, TypeNameMixin.class);
        m.addMixIn(MetadataModule.class, TypeNameMixin.class);
        m.addMixIn(SuggestModule.class, TypeNameMixin.class);
        m.addMixIn(MetricModule.class, TypeNameMixin.class);

        m.registerModule(serialization());

        return m;
    }

    @Override
    protected void configure() {
        bind(FilterJsonSerializer.class).toInstance(new FilterJsonSerializerImpl());
        bind(FilterJsonDeserializer.class).toInstance(new FilterJsonDeserializerImpl());

        bind(Scheduler.class).toInstance(new DefaultScheduler());
        bind(HeroicInternalLifeCycle.class).toInstance(lifeCycle);
        bind(FilterFactory.class).to(CoreFilterFactory.class).in(Scopes.SINGLETON);
        bind(FilterModifier.class).to(CoreFilterModifier.class).in(Scopes.SINGLETON);
        bind(QueryParser.class).to(CoreQueryParser.class).in(Scopes.SINGLETON);

        bind(HeroicConfigurationContext.class).to(CoreHeroicConfigurationContext.class).in(Scopes.SINGLETON);

        bind(HeroicContext.class).toInstance(new CoreHeroicContext());
        bind(ExecutorService.class).toInstance(executor);

        bind(JavaxRestFramework.class).toInstance(new CoreJavaxRestFramework());
    }

    public static Module serialization() {
        final SimpleModule serializers = new SimpleModule("serialization");
        serializers.addDeserializer(Duration.class, new DurationSerialization.Deserializer());
        serializers.addDeserializer(Groups.class, new GroupsSerialization.Deserializer());
        return serializers;
    }
}
package com.spotify.heroic;

import java.net.InetSocketAddress;
import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.common.CollectingTypeListener;
import com.spotify.heroic.common.IsSubclassOf;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.filter.FilterJsonDeserializer;
import com.spotify.heroic.filter.FilterJsonDeserializerImpl;
import com.spotify.heroic.filter.FilterJsonSerializer;
import com.spotify.heroic.filter.FilterJsonSerializerImpl;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.EventSerialization;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricGroupSerialization;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypeSerialization;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.MetricTypedGroupSerialization;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.PointSerialization;
import com.spotify.heroic.metric.Spread;
import com.spotify.heroic.metric.SpreadSerialization;
import com.spotify.heroic.statistics.HeroicReporter;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicPrimaryModule extends AbstractModule {
    private final HeroicCore core;
    private final Set<LifeCycle> lifeCycles;
    private final HeroicConfig config;
    private final InetSocketAddress bindAddress;

    private final boolean server;
    private final HeroicReporter reporter;
    private final HeroicStartupPinger pinger;

    @Provides
    @Singleton
    public HeroicCore core() {
        return core;
    }

    @Provides
    @Singleton
    public HeroicReporter reporter() {
        return reporter;
    }

    @Provides
    @Singleton
    public Set<LifeCycle> lifecycles() {
        return lifeCycles;
    }

    @Provides
    @Singleton
    public HeroicConfig config() {
        return config;
    }

    @Provides
    @Singleton
    @Named("bindAddress")
    public InetSocketAddress bindAddress() {
        return bindAddress;
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_JSON_INTERNAL)
    @Inject
    public ObjectMapper internalMapper(FilterJsonSerializer serializer, FilterJsonDeserializer deserializer,
            AggregationSerializer aggregationSerializer) {
        final SimpleModule module = new SimpleModule("custom");

        final FilterJsonSerializerImpl serializerImpl = (FilterJsonSerializerImpl) serializer;
        final FilterJsonDeserializerImpl deserializerImpl = (FilterJsonDeserializerImpl) deserializer;
        final CoreAggregationRegistry aggregationRegistry = (CoreAggregationRegistry) aggregationSerializer;

        deserializerImpl.configure(module);
        serializerImpl.configure(module);
        aggregationRegistry.configure(module);

        final ObjectMapper mapper = new ObjectMapper();

        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);
        mapper.addMixIn(AggregationQuery.class, TypeNameMixin.class);

        mapper.registerModule(module);
        mapper.registerModule(serializerModule());
        mapper.registerModule(new Jdk8Module());

        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_JSON)
    @Inject
    public ObjectMapper jsonMapper(@Named(HeroicCore.APPLICATION_JSON_INTERNAL) ObjectMapper mapper) {
        return mapper;
    }

    @Override
    protected void configure() {
        bind(QueryManager.class).to(CoreQueryManager.class).in(Scopes.SINGLETON);

        if (server) {
            bind(HeroicServer.class).in(Scopes.SINGLETON);
        }

        if (pinger != null) {
            bind(HeroicStartupPinger.class).toInstance(pinger);
        }

        bindListener(new IsSubclassOf(LifeCycle.class), new CollectingTypeListener<LifeCycle>(lifeCycles));
    }

    public static SimpleModule serializerModule() {
        final SimpleModule module = new SimpleModule("serializers");

        module.addSerializer(Point.class, new PointSerialization.Serializer());
        module.addDeserializer(Point.class, new PointSerialization.Deserializer());

        module.addSerializer(Event.class, new EventSerialization.Serializer());
        module.addDeserializer(Event.class, new EventSerialization.Deserializer());

        module.addSerializer(Spread.class, new SpreadSerialization.Serializer());
        module.addDeserializer(Spread.class, new SpreadSerialization.Deserializer());

        module.addSerializer(MetricGroup.class, new MetricGroupSerialization.Serializer());
        module.addDeserializer(MetricGroup.class, new MetricGroupSerialization.Deserializer());

        module.addSerializer(MetricTypedGroup.class, new MetricTypedGroupSerialization.Serializer());
        module.addDeserializer(MetricTypedGroup.class, new MetricTypedGroupSerialization.Deserializer());

        module.addSerializer(MetricType.class, new MetricTypeSerialization.Serializer());
        module.addDeserializer(MetricType.class, new MetricTypeSerialization.Deserializer());

        return module;
    }
}

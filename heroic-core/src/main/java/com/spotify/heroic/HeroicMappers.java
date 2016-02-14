/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.analytics.AnalyticsModule;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.DurationSerialization;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.GroupsSerialization;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.generator.MetadataGenerator;
import com.spotify.heroic.generator.MetricGeneratorModule;
import com.spotify.heroic.jetty.JettyConnectionFactory;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.EventSerialization;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricCollectionSerialization;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricGroupSerialization;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypeSerialization;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.PointSerialization;
import com.spotify.heroic.metric.Spread;
import com.spotify.heroic.metric.SpreadSerialization;
import com.spotify.heroic.suggest.SuggestModule;

/**
 * Contains factories for setting up ObjectMapper's for different purposes in Heroic.
 *
 * @author udoprog
 */
public final class HeroicMappers {
    /**
     * Setup the ObjectMapper used to deserialize configuration files.
     *
     * @return
     */
    public static ObjectMapper config() {
        final ObjectMapper m = new ObjectMapper(new YAMLFactory());

        m.addMixIn(ClusterDiscoveryModule.class, TypeNameMixin.class);
        m.addMixIn(RpcProtocolModule.class, TypeNameMixin.class);
        m.addMixIn(ConsumerModule.Builder.class, TypeNameMixin.class);
        m.addMixIn(MetadataModule.class, TypeNameMixin.class);
        m.addMixIn(SuggestModule.class, TypeNameMixin.class);
        m.addMixIn(MetricModule.class, TypeNameMixin.class);
        m.addMixIn(MetricGeneratorModule.class, TypeNameMixin.class);
        m.addMixIn(MetadataGenerator.class, TypeNameMixin.class);
        m.addMixIn(JettyConnectionFactory.Builder.class, TypeNameMixin.class);
        m.addMixIn(AnalyticsModule.Builder.class, TypeNameMixin.class);

        m.registerModule(commonSerializers());

        /* support Path */
        m.registerModule(new Jdk7Module());
        /* support Optional */
        m.registerModule(new Jdk8Module());

        return m;
    }

    public static ObjectMapper json() {
        final ObjectMapper mapper = new ObjectMapper();

        mapper.addMixIn(AggregationInstance.class, TypeNameMixin.class);
        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);

        mapper.registerModule(new Jdk7Module());
        mapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(true));
        mapper.registerModule(commonSerializers());
        mapper.registerModule(jsonSerializers());

        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return mapper;
    }

    public static Module commonSerializers() {
        final SimpleModule serializers = new SimpleModule("common");
        serializers.addDeserializer(Duration.class, new DurationSerialization.Deserializer());
        serializers.addDeserializer(Groups.class, new GroupsSerialization.Deserializer());
        return serializers;
    }

    public static SimpleModule jsonSerializers() {
        final SimpleModule module = new SimpleModule("serializers");

        module.addSerializer(Point.class, new PointSerialization.Serializer());
        module.addDeserializer(Point.class, new PointSerialization.Deserializer());

        module.addSerializer(Event.class, new EventSerialization.Serializer());
        module.addDeserializer(Event.class, new EventSerialization.Deserializer());

        module.addSerializer(Spread.class, new SpreadSerialization.Serializer());
        module.addDeserializer(Spread.class, new SpreadSerialization.Deserializer());

        module.addSerializer(MetricGroup.class, new MetricGroupSerialization.Serializer());
        module.addDeserializer(MetricGroup.class, new MetricGroupSerialization.Deserializer());

        module.addSerializer(MetricCollection.class,
            new MetricCollectionSerialization.Serializer());
        module.addDeserializer(MetricCollection.class,
            new MetricCollectionSerialization.Deserializer());

        module.addSerializer(MetricType.class, new MetricTypeSerialization.Serializer());
        module.addDeserializer(MetricType.class, new MetricTypeSerialization.Deserializer());

        return module;
    }
}

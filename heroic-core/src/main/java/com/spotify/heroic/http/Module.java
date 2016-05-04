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

package com.spotify.heroic.http;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.http.cluster.ClusterResource;
import com.spotify.heroic.http.metadata.MetadataResource;
import com.spotify.heroic.http.parser.ParserResource;
import com.spotify.heroic.http.query.QueryResource;
import com.spotify.heroic.http.render.RenderResource;
import com.spotify.heroic.http.status.StatusResource;
import com.spotify.heroic.http.utils.UtilsResource;
import com.spotify.heroic.http.write.WriteResource;
import dagger.Component;

import javax.inject.Inject;

public class Module implements HeroicModule {
    @Override
    public Entry setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    @Component(dependencies = CoreComponent.class)
    public static interface W {
        HeroicResource heroicResource();

        WriteResource writeResource();

        UtilsResource utilsResource();

        StatusResource statusResource();

        RenderResource renderResource();

        QueryResource queryResource();

        MetadataResource metadataResource();

        ClusterResource clusterResource();

        ParserResource parserResource();
    }

    static class E implements HeroicModule.Entry {
        private final HeroicConfigurationContext config;

        @Inject
        public E(HeroicConfigurationContext config) {
            this.config = config;
        }

        @Override
        public void setup() {
            config.resources(core -> {
                final W w = DaggerModule_W.builder().coreComponent(core).build();

                // @formatter:off
                return ImmutableList.of(
                    w.heroicResource(),
                    w.writeResource(),
                    w.utilsResource(),
                    w.statusResource(),
                    w.renderResource(),
                    w.queryResource(),
                    w.metadataResource(),
                    w.clusterResource(),
                    w.parserResource()
                );
                // @formatter:on
            });
        }
    }
}

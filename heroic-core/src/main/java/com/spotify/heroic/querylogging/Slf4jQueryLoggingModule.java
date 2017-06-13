/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.querylogging;

import static org.slf4j.event.Level.TRACE;

import com.spotify.heroic.dagger.PrimaryComponent;
import dagger.Module;
import dagger.Provides;
import java.util.Optional;
import java.util.function.Consumer;
import javax.inject.Named;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

@Data
@Module
public class Slf4jQueryLoggingModule implements QueryLoggingModule {
    private final Optional<String> name;
    private final Optional<String> level;

    @QueryLoggingScope
    @Provides
    @Named("logger")
    public Consumer<String> logger() {
        final String name = this.name.orElse("com.spotify.heroic.query_logging");
        final Level level = this.level.map(Level::valueOf).orElse(TRACE);

        final Logger logger = LoggerFactory.getLogger(name);

        switch (level) {
            case INFO:
                return logger::info;
            default:
            case TRACE:
                return logger::trace;
        }
    }

    @Override
    public Slf4jQueryLoggingComponent component(PrimaryComponent primary) {
        return DaggerSlf4jQueryLoggingComponent
            .builder()
            .primaryComponent(primary)
            .slf4jQueryLoggingModule(this)
            .build();
    }
}

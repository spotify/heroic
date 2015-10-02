package com.spotify.heroic;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicEarlyModule extends AbstractModule {
    private final HeroicConfig config;
    private final HeroicOptions options;

    @Provides
    @Singleton
    HeroicOptions options() {
        return options;
    }

    @Provides
    @Singleton
    public HeroicConfig config() {
        return config;
    }

    @Override
    protected void configure() {
    }
}
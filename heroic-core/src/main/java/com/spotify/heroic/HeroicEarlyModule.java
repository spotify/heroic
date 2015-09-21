package com.spotify.heroic;

import com.google.inject.AbstractModule;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicEarlyModule extends AbstractModule {
    private final HeroicConfig config;

    @Override
    protected void configure() {
    }
}
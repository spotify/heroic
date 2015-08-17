package com.spotify.heroic;

public interface HeroicCoreInjector {
    public <T> T inject(T instance);
}
package com.spotify.heroic;

public interface HeroicProfile {
    HeroicConfig build() throws Exception;

    String description();
}

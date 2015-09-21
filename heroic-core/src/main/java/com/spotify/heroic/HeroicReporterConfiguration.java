package com.spotify.heroic;

import com.spotify.heroic.statistics.HeroicReporter;

public interface HeroicReporterConfiguration {
    public void registerReporter(HeroicReporter reporter);
}
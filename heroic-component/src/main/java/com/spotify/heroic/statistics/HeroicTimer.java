package com.spotify.heroic.statistics;

import eu.toolchain.async.FutureFinished;

public interface HeroicTimer {
    public interface Context extends FutureFinished {
        long stop();
    }

    Context time();
}

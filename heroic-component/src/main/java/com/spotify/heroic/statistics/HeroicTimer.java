package com.spotify.heroic.statistics;

import com.spotify.heroic.async.Finishable;

public interface HeroicTimer {
    public interface Context extends Finishable {
        long stop();
    }

    Context time();
}

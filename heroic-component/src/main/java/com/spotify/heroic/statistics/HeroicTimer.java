package com.spotify.heroic.statistics;

import com.spotify.heroic.async.Callback;

public interface HeroicTimer {
    public interface Context extends Callback.Finishable {
        long stop();
    }

    Context time();
}

package com.spotify.heroic.statistics;

import eu.toolchain.async.FutureDone;

public interface FutureReporter {
    public static interface Context extends FutureDone<Object> {
    }

    Context setup();
}

package com.spotify.heroic.statistics;

import com.spotify.heroic.async.ObjectHandle;

public interface FutureReporter {
    public static interface Context extends ObjectHandle {
    }

    Context setup();
}

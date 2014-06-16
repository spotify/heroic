package com.spotify.heroic.statistics;

import com.spotify.heroic.async.Callback;

public interface CallbackReporter {
    public static interface Context extends Callback.Handle<Object> {
    }

    Context setup();
}

package com.spotify.heroic.injection;

public interface Lifecycle {
    public void start() throws Exception;

    public void stop() throws Exception;

    public boolean isReady();
}

package com.spotify.heroic.injection;

public interface LifeCycle {
    public void start() throws Exception;

    public void stop() throws Exception;

    public boolean isReady();
}

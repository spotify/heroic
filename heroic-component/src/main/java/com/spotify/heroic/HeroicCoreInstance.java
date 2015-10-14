package com.spotify.heroic;

public interface HeroicCoreInstance {
    public <T> T inject(T injectee);

    public <T> T injectInstance(Class<T> cls);

    public void shutdown();

    public void join() throws InterruptedException;
}
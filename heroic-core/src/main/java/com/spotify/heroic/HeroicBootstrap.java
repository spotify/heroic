package com.spotify.heroic;

/**
 * A bootstrapper, that is run immediately after the primary injector is ready.
 */
public interface HeroicBootstrap {
    public void run() throws Exception;
}
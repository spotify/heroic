package com.spotify.heroic;

import com.google.inject.Injector;

/**
 * Allows other components to inject more complex configuration to the heroic service.
 *
 * @author udoprog
 */
public interface HeroicConfigurator {
    public void setup(final Injector injector) throws Exception;
}
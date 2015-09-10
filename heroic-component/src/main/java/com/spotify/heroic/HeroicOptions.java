package com.spotify.heroic;

public interface HeroicOptions {
    /**
     * Local metric backends are disabled.
     */
    boolean isDisableLocal();

    /**
     * Is the current heroic instance a oneshot instance.
     *
     * Oneshot instances are where configuration and refreshing only happens once.
     */
    boolean isOneshot();
}

package com.spotify.heroic;

public interface ApplicationLifecycle {
    /**
     * Wait until the application has been fully started. This should be used on
     * any threads which are spawned to make sure that the application is ready
     * to process requests.
     *
     * @throws InterruptedException
     *             If initialization is interrupted.
     */
    public void awaitStartup() throws InterruptedException;
}

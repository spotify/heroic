package com.spotify.heroic;

public interface HeroicInternalLifeCycle {
    public static interface Context {
        public void registerShutdown(ShutdownHook hook);
    }

    public static interface ShutdownHook {
        public void onShutdown() throws Exception;
    }

    public static interface StartupHook {
        public void onStartup(HeroicInternalLifeCycle.Context context) throws Exception;
    }

    /**
     * Register a hook to be run at shutdown.
     *
     * @param name
     * @param runnable
     */
    public void registerShutdown(String name, ShutdownHook hook);

    /**
     * Register a hook to be run at startup.
     *
     * @param name
     * @param startup
     */
    public void register(String name, StartupHook hook);

    public void start();

    public void stop();
}
package com.spotify.heroic;

import org.apache.onami.scheduler.QuartzModule;

public class SchedulerModule extends QuartzModule {
    public static final class Config {
    }

    private final Config config;

    public SchedulerModule(final Config config) {
        this.config = config;
    }

    @Override
    protected void schedule() {
    }
}

package com.spotify.heroic;

import org.apache.onami.scheduler.QuartzModule;
import org.quartz.JobKey;

import com.spotify.heroic.scheduler.RefreshClusterJob;

public class HeroicSchedulerModule extends QuartzModule {
    private final String refreshClusterSchedule;

    public HeroicSchedulerModule(final String refreshClusterSchedule) {
        super();
        this.refreshClusterSchedule = refreshClusterSchedule;
    }

    public static final JobKey REFRESH_CLUSTER = JobKey
            .jobKey("refresh_cluster");

    @Override
    protected void schedule() {
        scheduleJob(RefreshClusterJob.class).withCronExpression(
                refreshClusterSchedule).withJobName(REFRESH_CLUSTER.getName());
    }
}
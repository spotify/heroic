package com.spotify.heroic;

import org.apache.onami.scheduler.QuartzModule;
import org.quartz.JobKey;

import com.spotify.heroic.scheduler.RefreshClusterJob;
import com.spotify.heroic.scheduler.RefreshTagsJob;

public class SchedulerModule extends QuartzModule {
    public static final JobKey REFRESH_TAGS = JobKey.jobKey("refresh_tags");
    public static final JobKey REFRESH_CLUSTER = JobKey.jobKey("refresh_cluster");

    /**
     * Schedule to trigger the job every 15 minutes
     */
    private static final String REFRESH_TAGS_SCHEDULE = "0 0 * * * ?";
    private static final String REFRESH_CLUSTER_SCHEDULE = "0 0 * * * ?";

    @Override
    protected void schedule() {
        scheduleJob(RefreshTagsJob.class).withCronExpression(
                REFRESH_TAGS_SCHEDULE).withJobName(REFRESH_TAGS.getName());
        scheduleJob(RefreshClusterJob.class).withCronExpression(
                REFRESH_CLUSTER_SCHEDULE).withJobName(REFRESH_CLUSTER.getName());
    }
}
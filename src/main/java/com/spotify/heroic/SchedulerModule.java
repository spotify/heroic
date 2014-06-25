package com.spotify.heroic;

import org.apache.onami.scheduler.QuartzModule;
import org.quartz.JobKey;

import com.spotify.heroic.scheduler.RefreshTagsJob;

public class SchedulerModule extends QuartzModule {
    public static final JobKey REFRESH_TAGS = JobKey.jobKey("refresh_tags");

    /**
     * Schedule to trigger the job every 15 minutes
     */
    private static final String REFRESH_TAGS_SCHEDULE = "0 0 * * * ?";

    @Override
    protected void schedule() {
        scheduleJob(RefreshTagsJob.class).withCronExpression(
                REFRESH_TAGS_SCHEDULE).withJobName(REFRESH_TAGS.getName());
    }
}
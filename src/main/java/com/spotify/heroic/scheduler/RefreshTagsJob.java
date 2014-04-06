package com.spotify.heroic.scheduler;

import javax.inject.Inject;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spotify.heroic.backend.TimeSeriesCache;

public class RefreshTagsJob implements Job {
    @Inject
    private TimeSeriesCache tagsCacheManager;

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        tagsCacheManager.refresh();
    }
}

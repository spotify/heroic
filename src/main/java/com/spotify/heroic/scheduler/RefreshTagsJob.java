package com.spotify.heroic.scheduler;

import javax.inject.Inject;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spotify.heroic.backend.TagsCacheManager;

public class RefreshTagsJob implements Job {
    @Inject
    private TagsCacheManager tagsCacheManager;

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        tagsCacheManager.refresh();
    }
}

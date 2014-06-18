package com.spotify.heroic.scheduler;

import javax.inject.Inject;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spotify.heroic.metadata.MetadataBackendManager;

public class RefreshTagsJob implements Job {
    @Inject
    private MetadataBackendManager metadata;

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        metadata.refresh();
    }
}

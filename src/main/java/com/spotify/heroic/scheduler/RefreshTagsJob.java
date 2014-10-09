package com.spotify.heroic.scheduler;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spotify.heroic.metadata.MetadataManager;

@Slf4j
public class RefreshTagsJob implements Job {
    @Inject
    private MetadataManager metadata;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Refreshing tags");
        metadata.refresh();
    }
}

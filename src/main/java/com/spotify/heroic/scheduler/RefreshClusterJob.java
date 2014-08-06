package com.spotify.heroic.scheduler;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spotify.heroic.cluster.ClusterManager;

@Slf4j
public class RefreshClusterJob implements Job {
    @Inject
    private ClusterManager cluster;

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        log.info("Refreshing cluster");

        try {
            cluster.refresh();
        } catch (Exception e) {
            log.error("Refresh failed", e);
        }
    }
}

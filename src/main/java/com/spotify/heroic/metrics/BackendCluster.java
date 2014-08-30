package com.spotify.heroic.metrics;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BackendCluster {
    private final int disabled;
    private final List<Backend> backends;

    public void execute(BackendOperation op) {
        for (final Backend b : backends) {
            try {
                op.run(disabled, b);
            } catch (final Exception e) {
                log.error("Backend operation failed", e);
            }
        }
    }
}

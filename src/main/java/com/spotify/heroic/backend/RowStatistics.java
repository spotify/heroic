package com.spotify.heroic.backend;

import lombok.Getter;

public class RowStatistics {
    @Getter
    private final int total;
    @Getter
    private final int failed;
    @Getter
    private final int successful;
    @Getter
    private final int cancelled;
    @Getter
    private final int cacheDuplicates;

    public RowStatistics(int successful, int failed, int cancelled) {
        this.total = failed + successful + cancelled;
        this.successful = successful;
        this.failed = failed;
        this.cancelled = cancelled;
        this.cacheDuplicates = 0;
    }

    public RowStatistics(int successful, int failed, int cancelled,
            int cacheDuplicates) {
        this.total = failed + successful + cancelled;
        this.successful = successful;
        this.failed = failed;
        this.cancelled = cancelled;
        this.cacheDuplicates = cacheDuplicates;
    }
}
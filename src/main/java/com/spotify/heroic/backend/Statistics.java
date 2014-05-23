package com.spotify.heroic.backend;

import lombok.Getter;
import lombok.ToString;

@ToString(of={"total", "failed", "successful", "cancelled", "cacheConflicts", "cacheDuplicates", "cacheHits"})
public class Statistics {
    @Getter
    private final int total;
    @Getter
    private final int failed;
    @Getter
    private final int successful;
    @Getter
    private final int cancelled;
    @Getter
    private final int cacheConflicts;
    @Getter
    private final int cacheDuplicates;
    @Getter
    private final int cacheHits;

    public Statistics(int successful, int failed, int cancelled) {
        this.total = failed + successful + cancelled;
        this.successful = successful;
        this.failed = failed;
        this.cancelled = cancelled;
        this.cacheConflicts = 0;
        this.cacheDuplicates = 0;
        this.cacheHits = 0;
    }

    public Statistics(int successful, int failed, int cancelled,
            int cacheConflicts, int cacheDuplicates, int cacheHits) {
        this.total = failed + successful + cancelled;
        this.successful = successful;
        this.failed = failed;
        this.cancelled = cancelled;
        this.cacheConflicts = cacheConflicts;
        this.cacheDuplicates = cacheDuplicates;
        this.cacheHits = cacheHits;
    }
}
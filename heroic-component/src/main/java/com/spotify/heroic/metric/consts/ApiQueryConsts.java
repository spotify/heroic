package com.spotify.heroic.metric.consts;

public class ApiQueryConsts {
    // TODO refactor these with BigtableMetricModule's copies of these consts
    /**
     * TODO describe reasoning
     */
    public static final int DEFAULT_MUTATE_RPC_TIMEOUT_MS = 4_000;

    /**
     * TODO describe reasoning
     */
    public static final int DEFAULT_READ_ROWS_RPC_TIMEOUT_MS = 4_000;

    /**
     * TODO describe reasoning
     */
    public static final int DEFAULT_SHORT_RPC_TIMEOUT_MS = 4_000 * 3;
}

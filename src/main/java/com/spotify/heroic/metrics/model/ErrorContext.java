package com.spotify.heroic.metrics.model;

/**
 * Describes which context the failure has.
 *
 * A context is used to determine the scope of the failure.
 *
 * If a specific error is related to a NODE, it means that all results for
 * that node is omitted.
 *
 * If a specific error is related to a SERIES, it means that all results for
 * that series is omitted.
 */
public enum ErrorContext {
    /**
     * An entire node failed.
     */
    NODE,
    /**
     * A single time series failed.
     */
    SERIES
}
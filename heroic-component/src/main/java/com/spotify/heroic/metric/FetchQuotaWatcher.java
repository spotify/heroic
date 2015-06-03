package com.spotify.heroic.metric;

public interface FetchQuotaWatcher {
    /**
     * Indicates that backend has read {@code n} more datapoints.
     *
     * @param n
     *            The number of datapoints read by the backend.
     * @return A {@code boolean} indicating weither the operation may continue or not.
     */
    public boolean readData(long n);

    /**
     * Indicates if readData quota has been breached or not.
     * 
     * @return {@code true} if there is data left to be read, {@code false} otherwise.
     */
    public boolean mayReadData();

    /**
     * Get how much data you are allowed to read.
     */
    public int getReadDataQuota();

    /**
     * Indicates if any quota has been violated.
     *
     * @return {@code true} if any quota was violated, {@code false} otherwise.
     */
    public boolean isQuotaViolated();
}
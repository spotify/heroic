package com.spotify.heroic.model;

public interface TimeData extends Comparable<TimeData> {
    public long getTimestamp();

    public int hash();

    boolean valid();
}

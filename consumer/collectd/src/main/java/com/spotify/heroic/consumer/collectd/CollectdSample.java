package com.spotify.heroic.consumer.collectd;

import java.util.List;

import lombok.Data;

@Data
public class CollectdSample {
    public static final int COUNTER = 0;
    public static final int GAUGE = 1;
    public static final int DERIVE = 2;
    public static final int ABSOLUTE = 3;

    private final String host;
    private final long time;
    private final String plugin;
    private final String pluginInstance;
    private final String type;
    private final String typeInstance;
    private final List<CollectdValue> values;
    private final long interval;
    private final String message;
    private final long severity;
}
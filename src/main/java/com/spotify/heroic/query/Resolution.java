package com.spotify.heroic.query;

import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

public class Resolution {
    @Getter
    @Setter
    private TimeUnit unit = TimeUnit.MINUTES;

    @Getter
    @Setter
    private long value = 5;
}

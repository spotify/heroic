package com.spotify.heroic.query;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;

public class AbsoluteDateRange implements DateRange {
    @Getter
    @Setter
    private long start;

    @Getter
    @Setter
    private long end;

    @Override
    public Date start() {
        return new Date(start);
    }

    @Override
    public Date end() {
        return new Date(end);
    }
}

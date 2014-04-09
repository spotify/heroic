package com.spotify.heroic.query;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "start", "end" })
public class AbsoluteDateRange implements DateRange {
    @Getter
    @Setter
    private long start;

    @Getter
    @Setter
    private long end;

    public AbsoluteDateRange() {
    }

    public AbsoluteDateRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public Date start() {
        return new Date(start);
    }

    @Override
    public Date end() {
        return new Date(end);
    }

    @Override
    public DateRange roundToInterval(long hint) {
        return new AbsoluteDateRange(start - (start % hint), end
                + (hint - (end % hint)));
    }
}

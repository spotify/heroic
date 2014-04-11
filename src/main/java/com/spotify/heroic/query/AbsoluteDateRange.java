package com.spotify.heroic.query;

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
    public long start() {
        return start;
    }

    @Override
    public long end() {
        return end;
    }

    @Override
    public long diff() {
        return end - start;
    }

    @Override
    public DateRange roundToInterval(long hint) {
        return new AbsoluteDateRange(start - (start % hint), end
                + (hint - (end % hint)));
    }
}
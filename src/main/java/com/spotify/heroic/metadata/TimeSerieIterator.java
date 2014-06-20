package com.spotify.heroic.metadata;

import java.util.Iterator;

import com.spotify.heroic.model.TimeSerie;

class TimeSerieIterator implements Iterator<TimeSerie> {
    private TimeSerie current;

    private final Iterator<TimeSerie> iterator;
    private final TimeSerieMatcher matcher;

    public TimeSerieIterator(Iterator<TimeSerie> iterator,
            TimeSerieMatcher matcher) {
        this.iterator = iterator;
        this.matcher = matcher;
    }

    private TimeSerie findNext() {
        while (iterator.hasNext()) {
            final TimeSerie next = iterator.next();

            if (!matcher.matches(next))
                continue;

            return next;
        }

        return null;
    }

    @Override
    public boolean hasNext() {
        if (this.current == null) {
            this.current = findNext();
        }

        return current != null;
    }

    @Override
    public TimeSerie next() {
        if (this.current == null) {
            this.current = findNext();
        }

        final TimeSerie current = this.current;

        this.current = null;

        return current;
    }

    @Override
    public void remove() {
        throw new RuntimeException("remove not supported");
    }
}
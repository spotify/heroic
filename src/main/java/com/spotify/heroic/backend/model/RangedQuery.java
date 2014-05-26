package com.spotify.heroic.backend.model;

import com.spotify.heroic.model.DateRange;

public interface RangedQuery<T> {
    T withRange(DateRange range);
    DateRange getRange();
}

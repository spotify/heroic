package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.common.DateRange;

public interface TaskQueryParameters {
    public List<String> getQuery();

    public DateRange getRange();

    public int getLimit();
}
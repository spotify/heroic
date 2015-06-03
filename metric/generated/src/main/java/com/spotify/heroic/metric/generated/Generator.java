package com.spotify.heroic.metric.generated;

import java.util.List;

import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.Series;

public interface Generator {
    List<DataPoint> generate(Series series, DateRange range, FetchQuotaWatcher watcher);

    List<Event> generateEvents(Series series, DateRange range, FetchQuotaWatcher watcher);
}
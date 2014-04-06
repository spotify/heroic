package com.spotify.heroic.query;

import java.util.Date;

public interface DateRange {
    Date start();

    Date end();

    DateRange roundToInterval(long hint);

}

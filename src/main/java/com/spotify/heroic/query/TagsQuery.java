package com.spotify.heroic.query;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "tags" })
public class TagsQuery {
    private static final DateRange DEFAULT_DATE_RANGE = new RelativeDateRange(
            TimeUnit.DAYS, 7);

    @Getter
    @Setter
    private Map<String, String> tags;

    @Getter
    @Setter
    private Set<String> only;
}

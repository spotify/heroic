package com.spotify.heroic.query;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "tags", "only" })
public class TagsQuery {
    @Getter
    @Setter
    private Map<String, String> tags;

    @Getter
    @Setter
    private Set<String> only;
}

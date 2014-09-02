package com.spotify.heroic.metadata;

import com.spotify.heroic.model.Series;

public final class MetadataUtils {
    public static String buildId(Series series) {
        return Integer.toHexString(series.hashCode());
    }
}

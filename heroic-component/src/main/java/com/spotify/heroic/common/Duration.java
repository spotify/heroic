package com.spotify.heroic.common;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.google.common.base.Optional;

/**
 * A helper type that represents a duration as the canonical duration/unit.
 *
 * This is provided so that we can implement a parser for it to simplify configurations that require durations.
 *
 * This type is intended to be conveniently de-serialize from a short-hand string type, like the following examples.
 *
 * <ul>
 * <li>1H - 1 Hour</li>
 * <li>5m - 5 minutes</li>
 * </ul>
 *
 * @author udoprog
 */
@Data
public class Duration {
    private final long duration;
    private final TimeUnit unit;

    public Duration(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = Optional.fromNullable(unit).or(TimeUnit.SECONDS);
    }

    public static Duration of(long duration, TimeUnit unit) {
        return new Duration(duration, unit);
    }
}
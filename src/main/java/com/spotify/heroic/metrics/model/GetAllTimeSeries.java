package com.spotify.heroic.metrics.model;

import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.TimeSerie;

/**
 * Results of one backend for get all rows request.
 * 
 */
@RequiredArgsConstructor
@ToString(of = { "timeSeries" })
public class GetAllTimeSeries {
    @Getter
    private final Set<TimeSerie> timeSeries;
}
package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.model.FindRowGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class FindRowGroupsReducer implements
        Callback.Reducer<FindRowGroups.Result, RowGroups> {
    @RequiredArgsConstructor
    public static final class PreparedGroup {
        @Getter
        private final MetricBackend backend;
        @Getter
        private final List<DataPointsRowKey> rows;
    }

    private final DateRange range;

    @Override
    public RowGroups done(Collection<FindRowGroups.Result> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception {
        return new RowGroups(prepareGroups(results));
    }

    private final Map<TimeSerieSlice, List<PreparedGroup>> prepareGroups(
            Collection<FindRowGroups.Result> results) throws QueryException {

        final Map<TimeSerieSlice, List<PreparedGroup>> queries = new HashMap<TimeSerieSlice, List<PreparedGroup>>();

        for (final FindRowGroups.Result result : results) {
            final MetricBackend backend = result.getBackend();

            for (final Map.Entry<TimeSerie, List<DataPointsRowKey>> entry : result
                    .getRowGroups().entrySet()) {
                final TimeSerieSlice slice = entry.getKey().slice(range);

                List<PreparedGroup> groups = queries.get(slice);

                if (groups == null) {
                    groups = new ArrayList<PreparedGroup>();
                    queries.put(slice, groups);
                }

                groups.add(new PreparedGroup(backend, entry.getValue()));
            }
        }

        return queries;
    }
}
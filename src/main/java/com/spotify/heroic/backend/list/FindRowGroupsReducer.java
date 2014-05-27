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
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public final class FindRowGroupsReducer implements
        Callback.Reducer<FindRows.Result, RowGroups> {
    @RequiredArgsConstructor
    public static final class PreparedGroup {
        @Getter
        private final MetricBackend backend;
        @Getter
        private final List<DataPointsRowKey> rows;
    }

    @Override
    public RowGroups done(Collection<FindRows.Result> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception {
        return new RowGroups(prepareGroups(results));
    }

    private final Map<TimeSerie, List<PreparedGroup>> prepareGroups(
            Collection<FindRows.Result> results) throws QueryException {

        final Map<TimeSerie, List<PreparedGroup>> queries = new HashMap<TimeSerie, List<PreparedGroup>>();

        for (final FindRows.Result result : results) {
            final MetricBackend backend = result.getBackend();

            for (final Map.Entry<TimeSerie, List<DataPointsRowKey>> entry : result
                    .getRowGroups().entrySet()) {
                final TimeSerie slice = entry.getKey();

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
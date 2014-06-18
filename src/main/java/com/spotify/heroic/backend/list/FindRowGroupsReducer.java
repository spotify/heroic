package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.model.PreparedGroup;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.kairosdb.DataPointsRowKey;
import com.spotify.heroic.metrics.model.FindRows;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public final class FindRowGroupsReducer implements
        Callback.Reducer<FindRows.Result, RowGroups> {
    @Override
    public RowGroups resolved(Collection<FindRows.Result> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
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
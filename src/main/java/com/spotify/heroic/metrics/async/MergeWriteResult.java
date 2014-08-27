package com.spotify.heroic.metrics.async;

import java.util.Collection;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.model.WriteResult;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class MergeWriteResult implements
        Callback.Reducer<WriteResult, WriteResult> {
    private static final MergeWriteResult instance = new MergeWriteResult();

    public static MergeWriteResult get() {
        return instance;
    }

    @Override
    public WriteResult resolved(Collection<WriteResult> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {
        WriteResult response = new WriteResult(0, errors, cancelled);

        for (final WriteResult result : results) {
            response = response.merge(result);
        }

        return response;
    }
}
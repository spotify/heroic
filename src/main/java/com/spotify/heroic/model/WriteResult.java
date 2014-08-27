package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Data;

import com.spotify.heroic.async.CancelReason;

@Data
@AllArgsConstructor
public class WriteResult {
    private final int successful;
    private final Collection<Exception> failed;
    private final Collection<CancelReason> cancelled;

    private static final Collection<Exception> EMPTY_FAILED = new ArrayList<>();
    private static final Collection<CancelReason> EMPTY_CANCELLED = new ArrayList<>();

    public WriteResult(int successful) {
        this(successful, EMPTY_FAILED, EMPTY_CANCELLED);
    }

    public WriteResult merge(final WriteResult other) {
        final ArrayList<Exception> failures = new ArrayList<>(this.failed);
        failures.addAll(other.failed);
        final ArrayList<CancelReason> cancels = new ArrayList<>(this.cancelled);
        cancels.addAll(other.cancelled);
        return new WriteResult(this.successful + other.successful, failures,
                cancels);
    }
}

package com.spotify.heroic.common;

import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public final class Optionals {
    public static <T> Optional<List<T>> mergeOptionalList(final Optional<List<T>> a, Optional<List<T>> b) {
        if (a.isPresent() && b.isPresent()) {
            return Optional.of(ImmutableList.copyOf(Iterables.concat(a.get(), b.get())));
        }

        return pickOptional(a, b);
    }

    public static <T> Optional<T> mergeOptional(Optional<T> a, Optional<T> b, BinaryOperator<T> merger) {
        if (a.isPresent() && b.isPresent()) {
            return Optional.of(merger.apply(a.get(), b.get()));
        }

        return pickOptional(a, b);
    }

    public static <T> Optional<T> pickOptional(Optional<T> a, Optional<T> b) {
        return b.isPresent() ? b : a;
    }
}
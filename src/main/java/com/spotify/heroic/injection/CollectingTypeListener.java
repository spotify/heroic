package com.spotify.heroic.injection;

import java.util.Collection;

import lombok.RequiredArgsConstructor;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

@RequiredArgsConstructor
public class CollectingTypeListener<T> implements TypeListener {
    private final Collection<T> collected;

    @Override
    public <I> void hear(final TypeLiteral<I> type,
            final TypeEncounter<I> encounter) {
        encounter.register(new InjectionListener<I>() {
            @SuppressWarnings("unchecked")
            @Override
            public void afterInjection(I i) {
                collected.add((T) i);
            }
        });
    }
}
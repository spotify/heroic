package com.spotify.heroic.injection;

import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

@RequiredArgsConstructor
public class LifeCycleTypeListener implements TypeListener {
    private final Set<LifeCycle> managed;

    @Override
    public <I> void hear(final TypeLiteral<I> type,
            final TypeEncounter<I> encounter) {
        encounter.register(new InjectionListener<I>() {
            @Override
            public void afterInjection(I i) {
                managed.add((LifeCycle) i);
            }
        });
    }
}
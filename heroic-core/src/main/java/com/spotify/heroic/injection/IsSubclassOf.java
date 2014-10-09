package com.spotify.heroic.injection;

import lombok.RequiredArgsConstructor;

import com.google.inject.TypeLiteral;
import com.google.inject.matcher.AbstractMatcher;

@RequiredArgsConstructor
public class IsSubclassOf extends AbstractMatcher<TypeLiteral<?>> {
    private final Class<?> type;

    @Override
    public boolean matches(TypeLiteral<?> t) {
        return type.isAssignableFrom(t.getRawType());
    }
}
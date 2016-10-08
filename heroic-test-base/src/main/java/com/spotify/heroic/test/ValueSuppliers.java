package com.spotify.heroic.test;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class ValueSuppliers {
    private final List<ValueSupplier> suppliers;

    public Optional<Object> lookup(
        final Type type, final boolean secondary, final String name
    ) {
        return suppliers
            .stream()
            .map(s -> s.supply(type, secondary, name))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

    public static ValueSuppliers empty() {
        return new ValueSuppliers(ImmutableList.of());
    }

    @FunctionalInterface
    public interface ValueSupplier {
        Optional<Object> supply(Type type, boolean secondary, String name);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<ValueSupplier> suppliers = new ArrayList<>();

        public Builder supplier(final ValueSupplier supplier) {
            suppliers.add(supplier);
            return this;
        }

        public ValueSuppliers build() {
            return new ValueSuppliers(ImmutableList.copyOf(suppliers));
        }
    }
}

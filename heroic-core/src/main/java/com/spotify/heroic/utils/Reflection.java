package com.spotify.heroic.utils;

import java.lang.reflect.Constructor;

public final class Reflection {
    public static <T> T buildInstance(String className, Class<T> expectedType) {
        final Class<?> clazz;

        try {
            clazz = Class.forName(className);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("No such class: " + className, e);
        }

        if (!expectedType.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Class is not subtype of: " + expectedType.getCanonicalName());
        }

        @SuppressWarnings("unchecked")
        final Class<T> target = (Class<T>) clazz;

        final Constructor<T> constructor;

        try {
            constructor = target.getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Cannot find empty constructor for class: " + target.getCanonicalName(),
                    e);
        }

        try {
            return constructor.newInstance();
        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            throw new IllegalArgumentException("Failed to create instance of class: " + target.getCanonicalName(), e);
        }
    }
}

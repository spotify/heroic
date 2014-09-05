package com.spotify.heroic.yaml;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import org.yaml.snakeyaml.TypeDescription;

public final class ConfigUtils {
    public static <T> T instance(String className, Class<T> expectedType)
            throws ValidationException {
        final Class<?> clazz;

        try {
            clazz = Class.forName(className);
        } catch (final ClassNotFoundException e) {
            throw new ValidationException("No such class: " + className, e);
        }

        if (!expectedType.isAssignableFrom(clazz)) {
            throw new ValidationException("Class is not subtype of: "
                    + expectedType.getCanonicalName());
        }

        @SuppressWarnings("unchecked")
        final Class<T> target = (Class<T>) clazz;

        final Constructor<T> constructor;

        try {
            constructor = target.getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw new ValidationException(
                    "Cannot find empty constructor for class: "
                            + target.getCanonicalName(), e);
        }

        try {
            return constructor.newInstance();
        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            throw new ValidationException(
                    "Failed to create instance of class: "
                            + target.getCanonicalName(), e);
        }
    }

    public static TypeDescription makeType(Class<?> clazz) {
        final Field field;

        try {
            field = clazz.getField("TYPE");
        } catch (final Exception e) {
            throw new RuntimeException("Invalid field 'TYPE' on class " + clazz);
        }

        final Object type;

        try {
            type = field.get(null);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Unable to access field 'TYPE' on class " + clazz);
        }

        final String stringType;

        try {
            stringType = (String) type;
        } catch (final ClassCastException e) {
            throw new RuntimeException("Type field 'TYPE' of class " + clazz
                    + " must be a String");
        }

        return new TypeDescription(clazz, stringType);
    }
}

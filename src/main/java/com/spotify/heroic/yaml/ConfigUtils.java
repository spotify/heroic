package com.spotify.heroic.yaml;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.TypeDescription;

public final class ConfigUtils {
    public static <T> T notNull(ConfigContext c, T object)
            throws ValidationException {
        if (object == null)
            throw new ValidationException("must be defined");

        return object;
    }

    public static String notEmpty(ConfigContext c, String string)
            throws ValidationException {
        if (string == null || string.isEmpty())
            throw new ValidationException("must be defined and non-empty");

        return string;
    }

    public static <T> List<T> notEmpty(ConfigContext c, List<T> list)
            throws ValidationException {
        if (list == null || list.isEmpty())
            throw new ValidationException("must be a non-empty list");

        return list;
    }

    public static Map<String, String> notEmpty(ConfigContext c,
            Map<String, String> map) throws ValidationException {
        if (map == null || map.isEmpty())
            throw new ValidationException("must be a non-empty map");

        return map;
    }

    public static URI toURI(ConfigContext c, String url)
            throws ValidationException {
        notEmpty(c, url);

        try {
            return new URI(url);
        } catch (final URISyntaxException e) {
            throw new ValidationException("must be a valid URL");
        }
    }

    public static Path toDirectory(ConfigContext c, String path)
            throws ValidationException {
        notEmpty(c, path);

        final Path p = Paths.get(path);

        if (!Files.isDirectory(p))
            throw new ValidationException("must be an existing directory");

        return p;
    }

    public static <T, V> Map<T, V> toMap(ConfigContext c, Map<T, V> map) {
        if (map == null)
            return new HashMap<T, V>();

        return map;
    }

    public static <T> List<T> toList(ConfigContext c, List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

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

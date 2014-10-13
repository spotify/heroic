package com.spotify.heroic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ModuleUtils {
    public static final String ENTRY_CLASS_NAME = "Entry";

    public static List<HeroicEntryPoint> loadModules(List<URL> moduleLocations) throws IOException {
        final ClassLoader loader = HeroicService.class.getClassLoader();

        final List<HeroicEntryPoint> modules = new ArrayList<>();

        for (URL input : moduleLocations) {
            final InputStream inputStream = input.openStream();

            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                modules.addAll(loadModule(loader, reader));
            }
        }

        return modules;
    }

    private static List<HeroicEntryPoint> loadModule(final ClassLoader loader, final BufferedReader reader)
            throws IOException {
        final List<HeroicEntryPoint> children = new ArrayList<>();

        while (true) {
            final String line = reader.readLine();

            if (line == null) {
                break;
            }

            final String packageName = line.trim();

            if (packageName.isEmpty()) {
                continue;
            }

            final String className = String.format("%s.%s", packageName, ENTRY_CLASS_NAME);

            final Class<?> clazz;

            try {
                clazz = loader.loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class '" + className + "' cannot be found for package '" + packageName
                        + "'", e);
            }

            if (!(HeroicEntryPoint.class.isAssignableFrom(clazz))) {
                throw new RuntimeException("Not a ModuleEntryPoint: " + clazz.toString());
            }

            final Constructor<?> constructor;

            try {
                constructor = clazz.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Expected empty constructor: " + clazz.toString(), e);
            } catch (SecurityException e) {
                throw new RuntimeException("Security exception when getting constructor for: " + clazz.toString(), e);
            }

            final HeroicEntryPoint entryPoint;

            try {
                entryPoint = (HeroicEntryPoint) constructor.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to create instance of: " + clazz.toString(), e);
            }

            children.add(entryPoint);
        }

        return children;
    }
}

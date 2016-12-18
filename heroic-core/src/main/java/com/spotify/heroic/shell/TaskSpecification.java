/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.shell;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.shell.protocol.Command;
import lombok.Data;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Data
public class TaskSpecification {
    private final String name;
    private final List<String> aliases;
    private final String usage;
    private final List<Command.Opt> options;
    private final List<Command.Arg> arguments;

    public Command toCommand() {
        return new Command(name, aliases, usage, options, arguments);
    }

    public static TaskSpecification fromClass(final Class<? extends ShellTask> task) {
        final TaskName n =
            Optional.ofNullable(task.getAnnotation(TaskName.class)).orElseThrow(() -> {
                return new IllegalArgumentException(
                    String.format("Task not annotated with @TaskName on %s",
                        task.getCanonicalName()));
            });

        final String name = n.value();
        final List<String> aliases = ImmutableList.copyOf(n.aliases());

        final String usage = Optional
            .ofNullable(task.getAnnotation(TaskUsage.class))
            .map(TaskUsage::value)
            .orElseGet(() -> {
                return String.format("<no @ShellTaskUsage annotation for %s>",
                    task.getCanonicalName());
            });

        final Optional<TaskParametersModel> taskParameters =
            Optional.ofNullable(task.getAnnotation(TaskParametersModel.class));

        final Optional<List<Class<?>>> parameterClasses =
            taskParameters.map(TaskParametersModel::value).map(TaskSpecification::traverseClasses);

        final List<Command.Opt> options = parameterClasses.map(classes -> {
            return classes.stream().flatMap(TaskSpecification::options).collect(toList());
        }).orElseGet(ImmutableList::of);

        final List<Command.Arg> arguments = parameterClasses.map(classes -> {
            return classes.stream().flatMap(TaskSpecification::arguments).collect(toList());
        }).orElseGet(ImmutableList::of);

        return new TaskSpecification(name, aliases, usage, options, arguments);
    }

    private static Stream<Command.Opt> options(final Class<?> cls) {
        final Stream.Builder<Command.Opt> parameters = Stream.builder();
        final Field[] fields = cls.getDeclaredFields();

        for (final Field field : fields) {
            final Option option = field.getAnnotation(Option.class);

            if (option == null) {
                continue;
            }

            final Optional<String> argument;

            if (field.getType().equals(boolean.class)) {
                argument = Optional.empty();
            } else {
                argument = Optional.of(field.getName());
            }

            final Optional<String> usage;

            if ("".equals(option.usage())) {
                usage = Optional.empty();
            } else {
                usage = Optional.of(option.usage());
            }

            final boolean optional = !option.required();

            final Command.Type type = typeFromMetavar(option.metaVar());

            parameters.add(
                new Command.Opt(option.name(), ImmutableList.copyOf(option.aliases()), type,
                    argument, usage, false, optional));
        }

        return parameters.build();
    }

    private static Stream<Command.Arg> arguments(final Class<?> cls) {
        final Stream.Builder<Command.Arg> arguments = Stream.builder();
        final Field[] fields = cls.getDeclaredFields();

        for (final Field field : fields) {
            final Argument argument = field.getAnnotation(Argument.class);

            if (argument == null) {
                continue;
            }

            final boolean list = field.getType().equals(List.class);
            final boolean optional = !argument.required();

            final String name = field.getName();
            final Command.Type type = typeFromMetavar(argument.metaVar());
            arguments.add(new Command.Arg(name, type, list, optional));
        }

        return arguments.build();
    }

    private static Command.Type typeFromMetavar(
        final String metaVar
    ) {
        switch (metaVar) {
            case "<file>":
                return new Command.Type.File();
            case "<file|->":
                return new Command.Type.FileOrStd();
            case "<group>":
                return new Command.Type.Group();
            case "<number>":
                return new Command.Type.Number();
            default:
                return new Command.Type.Any();
        }
    }

    private static List<Class<?>> traverseClasses(
        final Class<?> value
    ) {
        final Queue<Class<?>> queue = new LinkedList<>();
        queue.add(value);

        final Set<Class<?>> seen = new HashSet<>();
        final List<Class<?>> results = new ArrayList<>();

        while (!queue.isEmpty()) {
            final Class<?> next = queue.poll();

            if (seen.add(next)) {
                results.add(next);

                final Class<?> n = next.getSuperclass();

                if (n != null) {
                    queue.add(next.getSuperclass());
                }
            }
        }

        return results;
    }
}

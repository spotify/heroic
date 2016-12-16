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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

@Data
public class TaskSpecification {
    private final String name;
    private final List<String> aliases;
    private final String usage;

    public Command toCommand() {
        return new Command(name, aliases, usage);
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

        return new TaskSpecification(name, aliases, usage);
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

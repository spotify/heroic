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

package com.spotify.heroic.shell.protocol;

import com.google.common.collect.ImmutableList;
import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

import java.util.List;
import java.util.Optional;

@AutoSerialize
@Data
public class Command {
    final String name;
    final List<String> aliases;
    final String usage;
    final List<Opt> parameters;
    final List<Arg> arguments;

    public List<String> getAllNames() {
        return ImmutableList.<String>builder().add(name).addAll(aliases).build();
    }

    public Optional<Opt> getParameter(final String name) {
        return parameters
            .stream()
            .filter(p -> name.equals(p.getName()) || p.getAliases().stream().anyMatch(name::equals))
            .findFirst();
    }

    @AutoSerialize
    @Data
    public static class Opt {
        private final String name;
        private final List<String> aliases;
        private final Type type;
        private final Optional<String> argument;
        private final Optional<String> usage;
        private final boolean list;
        private final boolean optional;

        public String show() {
            return show(name);
        }

        public String show(final String name) {
            final String n = argument.map(arg -> name + " " + type.typeArgument(arg)).orElse(name);
            return list ? n + " [..]" : n;
        }

        public String display() {
            return display(name);
        }

        public String display(final String name) {
            String display = show();

            if (optional) {
                display = "[" + display + "]";
            }

            return display;
        }
    }

    @AutoSerialize
    @Data
    public static class Arg {
        private final String name;
        private final Type type;
        private final boolean list;
        private final boolean optional;

        public String show() {
            return list ? name + " [..]" : name;
        }

        public String display() {
            String display = show();

            if (optional) {
                display = "[" + display + "]";
            }

            return display;
        }
    }

    @AutoSerialize
    @AutoSerialize.SubTypes({
        @AutoSerialize.SubType(Type.Any.class), @AutoSerialize.SubType(Type.File.class),
        @AutoSerialize.SubType(Type.FileOrStd.class), @AutoSerialize.SubType(Type.Number.class),
        @AutoSerialize.SubType(Type.Group.class)
    })
    public interface Type {
        default String typeArgument(String name) {
            return name;
        }

        @AutoSerialize
        @Data
        class Any implements Type {
        }

        @AutoSerialize
        @Data
        class File implements Type {
            @Override
            public String typeArgument(final String name) {
                return "<file>";
            }
        }

        @AutoSerialize
        @Data
        class FileOrStd implements Type {
            @Override
            public String typeArgument(final String name) {
                return "<file|->";
            }
        }

        @AutoSerialize
        @Data
        class Number implements Type {
            @Override
            public String typeArgument(final String name) {
                return "<number>";
            }
        }

        @AutoSerialize
        @Data
        class Group implements Type {
            @Override
            public String typeArgument(final String name) {
                return "<group>";
            }
        }
    }
}

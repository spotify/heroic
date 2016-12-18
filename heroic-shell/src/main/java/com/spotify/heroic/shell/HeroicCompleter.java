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

import com.spotify.heroic.shell.protocol.Command;
import lombok.RequiredArgsConstructor;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

@RequiredArgsConstructor
public class HeroicCompleter implements Completer {
    final List<Command> definitions;
    final SortedSet<String> groups;

    @Override
    public void complete(
        final LineReader reader, final ParsedLine line, final List<Candidate> candidates
    ) {
        final List<String> words = line.words();

        if (line.wordIndex() <= 0) {
            definitions.forEach(d -> {
                candidates.add(new Candidate(d.getName()));
            });
        } else if (line.wordIndex() > 0) {
            final String commandName = words.get(0);

            findDefinition(commandName).ifPresent(d -> {
                final String previous = words.get(line.wordIndex() - 1);

                final Optional<Command.Opt> parameter = d.getParameter(previous);

                if (parameter.isPresent()) {
                    final Command.Opt p = parameter.get();

                    /* parameter has an argument */
                    if (p.getArgument().isPresent()) {
                        if (p.getType() instanceof Command.Type.Group) {
                            addGroups(candidates);
                        } else if (p.getType() instanceof Command.Type.File) {
                            addFiles(line.word(), candidates);
                        } else if (p.getType() instanceof Command.Type.FileOrStd) {
                            addFiles(line.word(), candidates);
                            candidates.add(candidate("-").build());
                        }

                        // TODO: understand type of parameters to give suggestions
                        return;
                    }
                }

                d.getArguments().forEach(argument -> {
                    candidates.add(candidate(argument.getName()).display(argument.show()).build());
                });

                d.getParameters().forEach(p -> {
                    candidates.add(candidate(p.getName()).display(p.show()).build());

                    p.getAliases().forEach(alias -> {
                        candidates.add(candidate(alias).display(p.show(alias)).build());
                    });
                });
            });
        }
    }

    private void addFiles(final String argument, final List<Candidate> candidates) {
        final String prefix;

        final int lastSeparator = argument.lastIndexOf('/');

        if (lastSeparator == -1) {
            prefix = "";
        } else {
            prefix = argument.substring(0, lastSeparator + 1);
        }

        final Path current = Paths.get(prefix);

        try (final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(current)) {
            for (final Path path : directoryStream) {
                final Candidate c;

                if (Files.isDirectory(path)) {
                    c = candidate(prefix + path.getFileName().toString() + "/")
                        .complete(false)
                        .build();
                } else {
                    c = candidate(prefix + path.getFileName().toString()).build();
                }

                candidates.add(c);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addGroups(final List<Candidate> candidates) {
        groups.forEach(group -> {
            candidates.add(new Candidate(group, group, null, null, null, null, true));
        });
    }

    private Optional<Command> findDefinition(final String name) {
        for (final Command d : definitions) {
            if (name.equals(d.getName())) {
                return Optional.of(d);
            }

            if (d.getAliases().stream().anyMatch(name::equals)) {
                return Optional.of(d);
            }
        }

        return Optional.empty();
    }

    public static CandidateBuilder candidate(final String value) {
        return new CandidateBuilder(value);
    }

    public static class CandidateBuilder {
        private final String value;

        private String display;
        private String group;
        private String description;
        private String suffix;
        private String key;
        private boolean complete = true;

        public CandidateBuilder(final String value) {
            this.value = value;
            this.display = value;
        }

        public CandidateBuilder display(final String display) {
            this.display = display;
            return this;
        }

        public CandidateBuilder description(final String description) {
            this.description = description;
            return this;
        }

        public CandidateBuilder complete(final boolean complete) {
            this.complete = complete;
            return this;
        }

        public Candidate build() {
            return new Candidate(value, display, group, description, suffix, key, complete);
        }
    }
}

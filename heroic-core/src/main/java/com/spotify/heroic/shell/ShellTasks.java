package com.spotify.heroic.shell;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import jline.console.ConsoleReader;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.shell.task.ConfigGet;
import com.spotify.heroic.shell.task.Configure;
import com.spotify.heroic.shell.task.Fetch;
import com.spotify.heroic.shell.task.Keys;
import com.spotify.heroic.shell.task.ListBackends;
import com.spotify.heroic.shell.task.MetadataCount;
import com.spotify.heroic.shell.task.MetadataDelete;
import com.spotify.heroic.shell.task.MetadataEntries;
import com.spotify.heroic.shell.task.MetadataFetch;
import com.spotify.heroic.shell.task.MetadataLoad;
import com.spotify.heroic.shell.task.MetadataMigrate;
import com.spotify.heroic.shell.task.MetadataMigrateSuggestions;
import com.spotify.heroic.shell.task.MetadataTags;
import com.spotify.heroic.shell.task.Query;
import com.spotify.heroic.shell.task.RepairNetworkMetadata;
import com.spotify.heroic.shell.task.SuggestKey;
import com.spotify.heroic.shell.task.SuggestPerformance;
import com.spotify.heroic.shell.task.SuggestTag;
import com.spotify.heroic.shell.task.SuggestTagKeyCount;
import com.spotify.heroic.shell.task.SuggestTagValue;
import com.spotify.heroic.shell.task.SuggestTagValues;
import com.spotify.heroic.shell.task.Write;
import com.spotify.heroic.shell.task.WritePerformance;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

public class ShellTasks {
    static final Map<String, TaskDefinition> available = new HashMap<>();

    static {
        // built-in
        available.put("exit", builtin("exit shell", (shell, reader, out, tasks, args) -> shell.exit()));
        available.put("help",
                builtin("print help", (shell, reader, out, tasks, args) -> shell.printTasksHelp(out, tasks)));
        available.put(
                "timeout",
                builtin("get/set shell timeout",
                        (shell, reader, out, tasks, args) -> shell.internalTimeoutTask(out, args)));
        available
                .put("clear", builtin("clear shell screen", (shell, reader, out, tasks, args) -> reader.clearScreen()));

        available.put("configure", shellTask(Configure.class));
        available.put("get", shellTask(ConfigGet.class));
        available.put("keys", shellTask(Keys.class));
        available.put("backends", shellTask(ListBackends.class));
        available.put("fetch", shellTask(Fetch.class));
        available.put("write", shellTask(Write.class));
        available.put("write-performance", shellTask(WritePerformance.class));
        available.put("metadata-delete", shellTask(MetadataDelete.class));
        available.put("metadata-fetch", shellTask(MetadataFetch.class));
        available.put("metadata-tags", shellTask(MetadataTags.class));
        available.put("metadata-count", shellTask(MetadataCount.class));
        available.put("metadata-entries", shellTask(MetadataEntries.class));
        available.put("metadata-migrate", shellTask(MetadataMigrate.class));
        available.put("metadata-migrate-suggestions", shellTask(MetadataMigrateSuggestions.class));
        available.put("metadata-load", shellTask(MetadataLoad.class));
        available.put("suggest-tag", shellTask(SuggestTag.class));
        available.put("suggest-key", shellTask(SuggestKey.class));
        available.put("suggest-tag-value", shellTask(SuggestTagValue.class));
        available.put("suggest-tag-values", shellTask(SuggestTagValues.class));
        available.put("suggest-tag-key-count", shellTask(SuggestTagKeyCount.class));
        available.put("suggest-performance", shellTask(SuggestPerformance.class));
        available.put("repair-network-metadata", shellTask(RepairNetworkMetadata.class));
        available.put("query", shellTask(Query.class));
    }

    public static Map<String, TaskDefinition> available() {
        return available;
    }

    static TaskDefinition builtin(final String usage, final Builtin command) {
        return new TaskDefinition() {
            @Override
            public TaskShellDefinition setup(HeroicCore core) throws Exception {
                return new TaskShellDefinition() {
                    @Override
                    public Task setup(final HeroicShellBridge shell, final ConsoleReader reader,
                            final SortedMap<String, Task> tasks) {
                        return core.inject(new Task() {
                            @Inject
                            AsyncFramework async;

                            @Override
                            public ShellTaskParams params() {
                                return null;
                            }

                            @Override
                            public String usage() {
                                return usage;
                            }

                            @Override
                            public AsyncFuture<Void> run(PrintWriter out, String[] args, ShellTaskParams params)
                                    throws Exception {
                                try {
                                    command.run(shell, reader, out, tasks, args);
                                } catch (Exception e) {
                                    return async.failed(e);
                                }

                                return async.resolved();
                            }
                        });
                    }
                };
            }
        };
    }

    static TaskDefinition shellTask(final Class<? extends ShellTask> task) {
        return new TaskDefinition() {
            @Override
            public TaskShellDefinition setup(final HeroicCore core) throws Exception {
                final ShellTask instance = core.inject(instance(task));

                final String usage;

                final ShellTaskUsage u = task.getAnnotation(ShellTaskUsage.class);

                if (u == null) {
                    usage = String.format("<no @ShellTaskUsage annotation for %s>", task.getName());
                } else {
                    usage = u.value();
                }

                return new TaskShellDefinition() {
                    @Override
                    public Task setup(HeroicShellBridge shell, ConsoleReader reader, SortedMap<String, Task> tasks) {
                        return new Task() {
                            @Override
                            public ShellTaskParams params() {
                                return instance.params();
                            }

                            @Override
                            public String usage() {
                                return usage;
                            }

                            @Override
                            public AsyncFuture<Void> run(PrintWriter out, String args[], ShellTaskParams params)
                                    throws Exception {
                                return instance.run(out, params);
                            }
                        };
                    }
                };
            };
        };
    }

    public static ShellTask instance(Class<? extends ShellTask> taskType) throws Exception {
        final Constructor<? extends ShellTask> constructor;

        try {
            constructor = taskType.getConstructor();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Task '" + taskType.getCanonicalName()
                    + "' does not have an accessible, empty constructor", e);
        }

        try {
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Failed to invoke constructor of '" + taskType.getCanonicalName(), e);
        }
    }

    public static interface Builtin {
        void run(HeroicShellBridge shell, ConsoleReader reader, PrintWriter out, SortedMap<String, Task> tasks,
                String[] args) throws Exception;
    }

    public static interface TaskDefinition {
        TaskShellDefinition setup(HeroicCore core) throws Exception;
    }

    public static interface TaskShellDefinition {
        Task setup(HeroicShellBridge shell, ConsoleReader reader, SortedMap<String, Task> tasks);
    }

    public static interface Task {
        ShellTaskParams params();

        String usage();

        AsyncFuture<Void> run(PrintWriter out, String[] args, ShellTaskParams params) throws Exception;
    }
}
package com.spotify.heroic.shell;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HeroicShellCommands {
    final List<HeroicShellCommand> commands;

    @JsonCreator
    public HeroicShellCommands(@JsonProperty("commands") List<HeroicShellCommand> commands) {
        this.commands = commands;
    }

    public static class HeroicShellCommand {
        final String command;
        final String usage;

        @JsonCreator
        public HeroicShellCommand(@JsonProperty("command") String command, @JsonProperty("usage") String usage) {
            this.command = checkNotNull(command, "command");
            this.usage = checkNotNull(usage, "usage");
        }
    }
}
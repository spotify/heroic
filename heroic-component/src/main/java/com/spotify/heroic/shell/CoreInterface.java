package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.shell.protocol.CommandDefinition;

public interface CoreInterface {
    public CoreShellInterface setup(ShellInterface shell) throws Exception;

    public List<CommandDefinition> getCommands() throws Exception;

    public void shutdown() throws Exception;
}
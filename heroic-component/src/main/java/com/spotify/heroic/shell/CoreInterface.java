package com.spotify.heroic.shell;

import java.util.List;

public interface CoreInterface {
    public CoreShellInterface setup(ShellInterface shell) throws Exception;

    public List<CommandDefinition> getCommands() throws Exception;

    public void shutdown() throws Exception;
}
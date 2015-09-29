package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.shell.protocol.CommandDefinition;

import eu.toolchain.async.AsyncFuture;

public interface CoreInterface {
    /**
     * Evaluate the given command.
     */
    public AsyncFuture<Void> evaluate(final List<String> command, final ShellIO io) throws Exception;

    /**
     * Shutdown this interface.
     */
    public void shutdown() throws Exception;

    /**
     * Get information about available commands.
     */
    public List<CommandDefinition> commands() throws Exception;
}
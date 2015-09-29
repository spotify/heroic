package com.spotify.heroic.shell;

import java.net.ServerSocket;
import java.util.Collection;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(exclude = { "commands", "tasks" })
class ShellServerState {
    final ServerSocket serverSocket;
    final Collection<ShellTaskDefinition> commands;
    final ShellTasks tasks;
}
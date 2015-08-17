package com.spotify.heroic.shell;

import java.net.ServerSocket;
import java.util.Collection;
import java.util.SortedMap;

import lombok.Data;


@Data
class ShellServerState {
    final ServerSocket serverSocket;
    final Collection<CoreTaskDefinition> commands;
    final SortedMap<String, CoreShellTaskDefinition> tasks;
}
package com.spotify.heroic.shell;

import java.net.ServerSocket;

import lombok.Data;

@Data
class ShellServerState {
    final ServerSocket serverSocket;
}
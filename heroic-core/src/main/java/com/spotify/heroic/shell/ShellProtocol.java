package com.spotify.heroic.shell;

import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;

public class ShellProtocol {
    public static SerializerFramework setupSerializer() {
        return TinySerializer.builder().build();
    }
}

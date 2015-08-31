package com.spotify.heroic.shell;

import com.spotify.heroic.shell.protocol.Request;
import com.spotify.heroic.shell.protocol.Request_Serializer;
import com.spotify.heroic.shell.protocol.Response;
import com.spotify.heroic.shell.protocol.Response_Serializer;

import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;

public class ShellProtocol {
    final SerializerFramework f = TinySerializer.builder().build();

    final Serializer<Request> request = new Request_Serializer(f);
    final Serializer<Response> response = new Response_Serializer(f);

    public SerializerFramework framework() {
        return f;
    }

    public Serializer<Request> buildRequest() {
        return request;
    }

    public Serializer<Response> buildResponse() {
        return response;
    }
}
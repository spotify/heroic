package com.spotify.heroic.serializers;

import java.nio.ByteBuffer;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.spotify.heroic.marshal.SafeUTF8Type;

public class SafeStringSerializer extends AbstractSerializer<String> {
    private static final SafeStringSerializer instance = new SafeStringSerializer();
    
    public static SafeStringSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(String obj) {
        return SafeUTF8Type.instance.decompose(obj);
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        return SafeUTF8Type.instance.compose(byteBuffer);
    }
}

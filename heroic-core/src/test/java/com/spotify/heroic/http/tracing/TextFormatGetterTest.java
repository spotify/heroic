package com.spotify.heroic.http.tracing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.primitives.UnsignedLongs;
import io.opencensus.contrib.http.util.HttpPropagationUtil;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceId;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.propagation.TextFormat;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TextFormatGetterTest {

  private final TextFormat textFormat = HttpPropagationUtil.getCloudTraceFormat();
  private TextFormatGetter<ContainerRequest> textFormatGetter = new TextFormatGetter<>();
  private ContainerRequest request;

  @Before
  public void setup() {
  request = mock(ContainerRequest.class);
}

  private static long spanIdToLong(SpanId spanId) {
    ByteBuffer buffer = ByteBuffer.allocate(SpanId.SIZE);
    buffer.put(spanId.getBytes());
    return buffer.getLong(0);
  }

  @Test
  public void testSpan () throws Exception {

    Random random = new Random(1234);
    SpanId generateSpanId = SpanId.generateRandomId(random);
    String spanId = UnsignedLongs.toString(spanIdToLong(generateSpanId));
    String traceId = TraceId.generateRandomId(random).toLowerBase16();

    final List<String> headers = new ArrayList<>();
    headers.add(traceId + "/" + spanId + ";o=1");
    doReturn(headers).when(request).getRequestHeader("X-Cloud-Trace-Context");

    final SpanContext spanContext = textFormat.extract(request, textFormatGetter);

    assertEquals(generateSpanId, spanContext.getSpanId());
  }

  @Test
  public void testNullSpan () {

    final List<String> headers = new ArrayList<>();

    doReturn(headers).when(request).getRequestHeader(Mockito.anyString());

    try {
      textFormat.extract(request, textFormatGetter);
    } catch (Exception e) {
      assertEquals(e.getClass(), SpanContextParseException.class);
    }
  }

}

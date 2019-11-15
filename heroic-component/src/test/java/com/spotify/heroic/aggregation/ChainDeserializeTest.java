package com.spotify.heroic.aggregation;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class ChainDeserializeTest {

  private ObjectMapper mapper;

  @Before
  public void setup() {
    mapper = new ObjectMapper();
  }

  @Test
  public void testDeserialize() throws Exception {
    assertEquals(Chain.class, mapper.readValue("{\"chain\":[]}", Chain.class).getClass());

  }

}

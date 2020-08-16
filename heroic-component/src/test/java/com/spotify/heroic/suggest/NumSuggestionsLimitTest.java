package com.spotify.heroic.suggest;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.FalseFilter;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NumSuggestionsLimitTest {
  public static final int LIMIT_FORTY = 40;
  private NumSuggestionsLimit limit;

  @Before
  public void setup() {
    this.limit = NumSuggestionsLimit.of(LIMIT_FORTY);
  }

  @Test
  public void TestCorrectLimitIsApplied() {

    // Check that a supplied limit is selected
    int result = this.limit.calculateNewLimit(OptionalLimit.of(5));
    assertEquals(5, result);

    // Check that none of the above have affected the NSL's stored number
    result = this.limit.calculateNewLimit(OptionalLimit.empty());
    assertEquals(LIMIT_FORTY, result);

    // Check that a giant request limit is not respected
    result = this.limit.calculateNewLimit(OptionalLimit.of(10_000));
    assertEquals(NumSuggestionsLimit.LIMIT_CEILING, result);

    // Check that none of the above have affected the NSL's stored number. The
    // above operations return a new object each time.
    assertEquals(LIMIT_FORTY, this.limit.getLimit());
  }
}

package com.spotify.heroic.model.filter;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;

public class AndFilterTest {
	@Test
	public void testOptimize() throws Exception {
		final Filter reference = Mockito.mock(Filter.class);
		Mockito.when(reference.optimize()).thenReturn(reference);

		final AndFilter nested = new AndFilter(Arrays.asList(new Filter[] {}));
		final AndFilter and1 = new AndFilter(
				Arrays.asList(new Filter[] { nested }));
		final AndFilter and2 = new AndFilter(
				Arrays.asList(new Filter[] { and1 }));

		Assert.assertEquals(nested, and2.optimize());
	}
}

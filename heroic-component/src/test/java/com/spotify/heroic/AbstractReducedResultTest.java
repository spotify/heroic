package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.RequestError;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractReducedResultTest {
    @Mock
    protected RequestError e1;
    @Mock
    protected RequestError e2;

    protected List<RequestError> errors;

    @Before
    public final void abstractSetup() {
        errors = ImmutableList.of(e1, e2);
    }
}

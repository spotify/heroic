package com.spotify.heroic.elasticsearch;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend.ScrollTransformStream;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

@RunWith(MockitoJUnitRunner.class)
public class ScrollTransformStreamTest {
    private final String scrollID = "scroller1";

    @Mock
    private AsyncFramework async;

    @Mock
    private AsyncFuture<ScrollTransformResult> resolved;

    @Mock
    private Supplier<AsyncFuture<SearchResponse>> scroller;

    @Mock
    private Function<Set<SearchHit>, AsyncFuture<Void>> seriesFunction;

    @Mock
    private AsyncFuture<Void> handleResultFuture;

    @Mock
    private Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollerFactory;

    @Mock
    private AsyncFuture<SearchResponse> response;

    @Mock
    private SearchResponse searchResponse;

    @Mock
    private SearchHits searchHits;

    private List<SearchHit> allSearchResults = new ArrayList<>();

    private final SearchHit[] searchHits1 = {
        mock(SearchHit.class), mock(SearchHit.class), mock(SearchHit.class),
    };

    private final SearchHit[] searchHits2 = {
        mock(SearchHit.class), mock(SearchHit.class), mock(SearchHit.class),
    };

    private final SearchHit[] emptySearchHits = {};

    @Before
    public void setup() {
        doReturn(searchHits).when(searchResponse).getHits();
        doReturn(scrollID).when(searchResponse).getScrollId();

        doReturn(scroller).when(scrollerFactory).apply(any(String.class));
        doReturn(response).when(scroller).get();
        doAnswer(new Answer<AsyncFuture<ScrollTransformResult<Integer>>>() {
            public AsyncFuture<ScrollTransformResult<Integer>> answer(
                InvocationOnMock invocation
            ) throws Exception {
                final LazyTransform<SearchResponse, ScrollTransformResult<Integer>> transform =
                    (LazyTransform<SearchResponse, ScrollTransformResult<Integer>>) invocation.getArguments
                        ()[0];
                return transform.transform(searchResponse);
            }
        }).when(response).lazyTransform(any(LazyTransform.class));

        doAnswer(new Answer<AsyncFuture<Void>>() {
            public AsyncFuture<Void> answer(InvocationOnMock invocation) throws Exception {
                final Set<SearchHit> hits = (Set<SearchHit>) invocation.getArguments()[0];
                allSearchResults.addAll(hits);
                return handleResultFuture;
            }
        }).when(seriesFunction).apply(any());

        doAnswer(new Answer<AsyncFuture<ScrollTransformResult<Integer>>>() {
            public AsyncFuture<ScrollTransformResult<Integer>> answer(
                InvocationOnMock invocation
            ) throws Exception {
                final LazyTransform<Void, ScrollTransformResult<Integer>> transform =
                    (LazyTransform<Void, ScrollTransformResult<Integer>>) invocation.getArguments()[0];
                final Void ignore = null;
                return transform.transform(ignore);
            }
        }).when(handleResultFuture).lazyTransform(any(LazyTransform.class));
    }

    public void setSearchHitPages(SearchHit[]... pages) {
        OngoingStubbing<SearchHit[]> stub = when(searchHits.getHits());

        for (final SearchHit[] page : pages) {
            stub = stub.thenReturn(page);
        }
    }

    public ScrollTransformStream<SearchHit> createScrollTransform(
        Integer limit, Function<Set<SearchHit>, AsyncFuture<Void>> seriesFunction,
        Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollerFactory
    ) {
        final OptionalLimit optionalLimit = OptionalLimit.of(limit);
        return new ScrollTransformStream<>(optionalLimit, seriesFunction, Function.identity(),
            scrollerFactory);
    }

    public List<SearchHit> createExpectedResult(Integer limit, SearchHit[]... pages) {
        final List<SearchHit> hits = new ArrayList<>();

        int num = 0;
        for (final SearchHit[] page : pages) {

            final Set<SearchHit> pageSet = new HashSet<>();
            for (final SearchHit s : page) {
                if (limit != null && num >= limit) {
                    break;
                }
                num++;
                pageSet.add(s);
            }

            // Add SearchHit's to the list, filtered through a Set to get the right output
            for (final SearchHit s : pageSet) {
                hits.add(s);
            }

            if (limit != null && num >= limit) {
                break;
            }
        }

        return hits;
    }

    @Test
    public void aboveLimit() throws Exception {
        setSearchHitPages(searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length - 1;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(limit, searchHits1), allSearchResults);
        verify(scroller, times(1)).get();
    }

    @Test
    public void atLimit() throws Exception {
        setSearchHitPages(searchHits1, emptySearchHits);
        final Integer limit = searchHits1.length;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(null, searchHits1), allSearchResults);
        verify(scroller, times(1)).get();
    }

    @Test
    public void belowLimit() throws Exception {
        setSearchHitPages(searchHits1, emptySearchHits);
        final Integer limit = searchHits1.length + 1;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(null, searchHits1), allSearchResults);
        verify(scroller, times(2)).get();
    }

    @Test
    public void aboveLimitWithDuplicates() throws Exception {
        setSearchHitPages(searchHits1, searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length + searchHits1.length + searchHits2.length - 1;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(limit, searchHits1, searchHits1, searchHits2),
            allSearchResults);
        verify(scroller, times(3)).get();
    }

    @Test
    public void atLimitWithDuplicates() throws Exception {
        setSearchHitPages(searchHits1, searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length + searchHits1.length + searchHits2.length;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(limit, searchHits1, searchHits1, searchHits2),
            allSearchResults);
        verify(scroller, times(3)).get();
    }

    @Test
    public void belowLimitWithDuplicates() throws Exception {
        setSearchHitPages(searchHits1, searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length + searchHits1.length + searchHits2.length + 1;

        final ScrollTransformStream<SearchHit> scrollTransform =
            createScrollTransform(limit, seriesFunction, scrollerFactory);

        scroller.get().lazyTransform(scrollTransform);

        assertEquals(createExpectedResult(limit, searchHits1, searchHits1, searchHits2),
            allSearchResults);
        verify(scroller, times(4)).get();
    }
}

package com.spotify.heroic.elasticsearch;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metadata.FindSeries;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractElasticsearchMetadataBackend {
    public static final TimeValue SCROLL_TIME = TimeValue.timeValueSeconds(5);

    private final AsyncFramework async;

    protected <T> AsyncFuture<T> bind(final ListenableActionFuture<T> actionFuture) {
        final ResolvableFuture<T> future = async.future();

        actionFuture.addListener(new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable e) {
                future.fail(e);
            }
        });

        return future;
    }

    protected AsyncFuture<FindSeries> scrollOverSeries(final Connection c, final SearchRequestBuilder request,
            final long limit, final Function<SearchHit, Series> converter) {
        return bind(request.execute()).lazyTransform((initial) -> {
            if (initial.getScrollId() == null) {
                return async.resolved(FindSeries.EMPTY);
            }

            return bind(c.prepareSearchScroll(initial.getScrollId()).setScroll(SCROLL_TIME).execute()).lazyTransform((response) -> {
                final ResolvableFuture<FindSeries> future = async.future();
                final Set<Series> series = new HashSet<>();
                final AtomicInteger count = new AtomicInteger();

                final Consumer<SearchResponse> consumer = new Consumer<SearchResponse>() {
                    @Override
                    public void accept(final SearchResponse response) {
                        final SearchHit[] hits = response.getHits().hits();

                        for (final SearchHit hit : hits) {
                            series.add(converter.apply(hit));
                        }

                        count.addAndGet(hits.length);

                        if (hits.length == 0 || count.get() >= limit || response.getScrollId() == null) {
                            future.resolve(new FindSeries(series, series.size(), 0));
                            return;
                        }

                        bind(c.prepareSearchScroll(response.getScrollId()).setScroll(SCROLL_TIME).execute()).on(new FutureDone<SearchResponse>() {
                            @Override
                            public void failed(Throwable cause) throws Exception {
                                future.fail(cause);
                            }

                            @Override
                            public void resolved(SearchResponse result) throws Exception {
                                accept(result);
                            }

                            @Override
                            public void cancelled() throws Exception {
                                future.cancel();
                            }
                        });
                    }
                };

                consumer.accept(response);

                return future;
            });
        });
    }
}
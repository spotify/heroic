package com.spotify.heroic.backend;

/**
 * Helper class that allows for safer Runnable implementations meant to wrap
 * Query<T>
 * 
 * @author udoprog
 * @param <T>
 *            The type expected to be returned by the implemented execute
 *            function and realized for the specified query.
 */
public abstract class QueryRunnable<T> implements Runnable {
    private final Query<T> query;

    public QueryRunnable(Query<T> query) {
        this.query = query;
    }

    @Override
    public void run() {
        final T result;

        try {
            result = execute();
        } catch (Throwable t) {
            query.fail(t);
            return;
        }

        query.finish(result);
    }

    public abstract T execute() throws Exception;
}

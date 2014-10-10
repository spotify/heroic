package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Gives access to static common reducers to reduce overhead for common tasks.
 *
 * @author udoprog
 */
public final class Reducers {
    private interface ListReducer<T> extends Reducer<T, List<T>> {
    }

    private static final ListReducer<Object> LIST = new ListReducer<Object>() {
        @Override
        public List<Object> resolved(Collection<Object> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            return new ArrayList<Object>(results);
        }
    };

    /**
     * A reducer that maps T -> List<T>.
     *
     * @return The reducer.
     */
    @SuppressWarnings("unchecked")
    public static <T> Reducer<T, List<T>> list() {
        return (ListReducer<T>) LIST;
    }

    private interface JoinSets<T> extends Reducer<Set<T>, Set<T>> {
    }

    private static final JoinSets<Object> JOIN_SETS = new JoinSets<Object>() {
        @Override
        public Set<Object> resolved(Collection<Set<Object>> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            final Set<Object> all = new HashSet<Object>();

            for (final Set<Object> result : results) {
                all.addAll(result);
            }

            return all;
        }
    };

    /**
     * A reducer that maps Set<T> -> Set<T> using a join operations.
     *
     * @return The reducer.
     */
    @SuppressWarnings("unchecked")
    public static <T> Reducer<Set<T>, Set<T>> joinSets() {
        return (JoinSets<T>) JOIN_SETS;
    }

    private interface JoinLists<T> extends Reducer<List<T>, List<T>> {
    }

    private static final JoinLists<Object> JOIN_LISTS = new JoinLists<Object>() {
        @Override
        public List<Object> resolved(Collection<List<Object>> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            final List<Object> list = new ArrayList<Object>();

            for (final List<Object> part : results) {
                list.addAll(part);
            }

            return list;
        }

    };

    @SuppressWarnings("unchecked")
    public static <T> Reducer<List<T>, List<T>> joinLists() {
        return (JoinLists<T>) JOIN_LISTS;
    }

    private static final Reducer<Object, Void> TO_VOID = new Reducer<Object, Void>() {
        @Override
        public Void resolved(Collection<Object> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            return null;
        }
    };

    /**
     * A reducer that maps T -> Void.
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Reducer<T, Void> toVoid() {
        return (Reducer<T, Void>) TO_VOID;
    }

    private static final Reducer<Boolean, Boolean> ALL = new Reducer<Boolean, Boolean>() {
        @Override
        public Boolean resolved(Collection<Boolean> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            if (!errors.isEmpty() || !cancelled.isEmpty()) {
                throw new Exception("Not all callbacks were resolved");
            }

            for (final Boolean b : results) {
                if (!b) {
                    return false;
                }
            }

            return true;
        }
    };

    public static Reducer<Boolean, Boolean> all() {
        return ALL;
    }
}

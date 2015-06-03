package com.spotify.heroic.filter;

import java.util.List;

public interface Filter extends Comparable<Filter> {
    public interface MultiArgs<A> extends Filter {
        public List<A> terms();
    }

    public interface NoArg extends Filter {
        Filter invert();
    }

    public interface OneArg<A> extends Filter {
        public A first();
    }

    public interface TwoArgs<A, B> extends Filter {
        public A first();

        public B second();
    }

    /**
     * Concrete interfaces for filters.
     *
     * These are necessary when writing converters that typically check instance types.
     **/
    public interface Or extends MultiArgs<Filter> {
    }

    public interface And extends MultiArgs<Filter> {
    }

    public interface True extends NoArg {
    }

    public interface False extends NoArg {
    }

    public interface HasTag extends OneArg<String> {
    }

    public interface MatchKey extends OneArg<String> {
    }

    public interface MatchTag extends TwoArgs<String, String> {
    }

    public interface Not extends OneArg<Filter> {
    }

    public interface StartsWith extends TwoArgs<String, String> {
    }

    public interface Regex extends TwoArgs<String, String> {
    }

    public interface Raw extends OneArg<String> {
    }

    public Filter optimize();

    public String operator();
}

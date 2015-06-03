package com.spotify.heroic.filter;

/**
 * Provides utility functions for modifying filters.
 *
 * @author udoprog
 */
public interface FilterModifier {
    /**
     * Remove any mentions of a specific tag (recursively) from the specified filter.
     *
     * @param filter
     *            Filter to modify.
     * @param tag
     *            Tag to remove.
     * @return A modified filter that does not mention the specified tag.
     */
    public Filter removeTag(Filter filter, String tag);
}

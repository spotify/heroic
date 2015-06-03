package com.spotify.heroic.suggest;

import com.spotify.heroic.utils.GroupManager;

/**
 * Interface for handling tag suggestions.
 *
 * Suggestions can be a potentially complex system, it is separated from metadata to allow for a clean separation of
 * concerns, and deployments where suggestions _may_ be broken independently of metadata.
 */
public interface SuggestManager extends GroupManager<SuggestBackend, SuggestBackend> {
}
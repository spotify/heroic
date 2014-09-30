package com.spotify.heroic.exceptions;

import com.spotify.heroic.metric.model.NodeError;

/**
 * An exception which is caused by the user of the system.
 *
 * This is used by
 * {@link NodeError#fromException(NodeError.Context, java.util.Map, Exception)}
 *
 * @author udoprog
 */
public class UserException extends Exception {
    private static final long serialVersionUID = 2790193805521892399L;

    public UserException(String message) {
        super(message);
    }
}

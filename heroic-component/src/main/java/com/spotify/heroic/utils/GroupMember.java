package com.spotify.heroic.utils;

import java.util.Set;

import lombok.Data;

@Data
public class GroupMember<T> {
    private final T member;
    private final Set<String> groups;
    private final boolean defaultMember;
}
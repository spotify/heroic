package com.spotify.heroic.suggest;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;

import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.utils.BackendGroups;
import com.spotify.heroic.utils.GroupMember;

import eu.toolchain.async.AsyncFramework;

@NoArgsConstructor
public class LocalSuggestManager implements SuggestManager {
    @Inject
    private AsyncFramework async;

    @Inject
    @Named("backends")
    private BackendGroups<SuggestBackend> backends;

    @Inject
    private LocalMetadataManagerReporter reporter;

    @Override
    public List<GroupMember<SuggestBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public SuggestBackend useDefaultGroup() throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.useDefault(), reporter);
    }

    @Override
    public SuggestBackend useGroup(String group) throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.use(group), reporter);
    }

    @Override
    public SuggestBackend useGroups(Set<String> groups) throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.use(groups), reporter);
    }
}
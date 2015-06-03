package com.spotify.heroic.metadata;

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
public class LocalMetadataManager implements MetadataManager {
    @Inject
    private AsyncFramework async;

    @Inject
    @Named("backends")
    private BackendGroups<MetadataBackend> backends;

    @Inject
    private LocalMetadataManagerReporter reporter;

    @Override
    public List<GroupMember<MetadataBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public MetadataBackend useDefaultGroup() throws BackendGroupException {
        return new MetadataBackendGroup(backends.useDefault(), async, reporter);
    }

    @Override
    public MetadataBackend useGroup(String group) throws BackendGroupException {
        return new MetadataBackendGroup(backends.use(group), async, reporter);
    }

    @Override
    public MetadataBackend useGroups(Set<String> groups) throws BackendGroupException {
        return new MetadataBackendGroup(backends.use(groups), async, reporter);
    }
}
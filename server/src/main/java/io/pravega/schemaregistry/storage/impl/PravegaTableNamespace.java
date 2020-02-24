package io.pravega.schemaregistry.storage.impl;

import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.GroupProperties;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaTableNamespace implements Namespace {
    private final PravegaTablesStoreHelper tablesStoreHelper;
    private final ScheduledExecutorService executor;
    private final String namespaceId;
    
    public PravegaTableNamespace(String id, PravegaTablesStoreHelper tablesStoreHelper, ScheduledExecutorService executor) {
        this.tablesStoreHelper = tablesStoreHelper;
        this.executor = executor;
        this.namespaceId = id;
    }

    @Override
    public CompletableFuture<Group> getGroup(String groupName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> addNewGroup(String group, GroupProperties groupProperties) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> getGroups() {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        return null;
    }
}

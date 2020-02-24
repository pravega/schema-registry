package io.pravega.schemaregistry.storage.impl.namespace;

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.group.Group;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaTableNamespace implements Namespace {
    private static final String GROUPS_IN_NAMESPACE = "GroupsInNamespace_%s";
    private final TableStore tablesStore;
    private final ScheduledExecutorService executor;
    private final String tableName;
    
    public PravegaTableNamespace(String id, TableStore tablesStore, ScheduledExecutorService executor) {
        this.tablesStore = tablesStore;
        this.executor = executor;
        this.tableName = String.format(GROUPS_IN_NAMESPACE, id);
    }

    CompletableFuture<Boolean> checkNamespaceExist() {
        return tablesStore.checkTableExists(tableName);
    }
    
    @Override
    public CompletableFuture<Group> getGroup(String groupName) {
        return tablesStore.getEntry(tableName, groupName, );
    }

    @Override
    public CompletableFuture<Boolean> addNewGroup(String group, GroupProperties groupProperties) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> getGroups() {
        // read from namespace's table
        List<String> namespaces = new LinkedList<>();
        return tablesStore.getAllKeys(tableName).collectRemaining(namespaces::add)
                          .thenApply(v -> new ListWithToken<>(namespaces, null));

    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        return null;
    }
}

package io.pravega.schemaregistry.service;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.pravega.schemaregistry.contract.Compatibility;
import io.pravega.schemaregistry.contract.SchemaValidationRules;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingId;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.GroupProperties;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaWithVersion;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.VersionInfo;

public interface SchemaRegistryService {
    CompletableFuture<Void> createScope(String scope);
    
    CompletableFuture<Boolean> createGroup(String scope, String groupId, SchemaType type, Compatibility compatibility, boolean allowSubgroups, boolean encodeHeader);

    CompletableFuture<GroupProperties> getGroupProperties(String scope, String groupId);

    CompletableFuture<List<String>> getSubgroups(String scope, String groupId);

    CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema,
                                                         SchemaValidationRules rules);

    CompletableFuture<SchemaInfo> getSchema(String scope, String groupId, @Nullable String subgroupId, VersionInfo version);

    CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String groupId, EncodingId encodingId);

    CompletableFuture<EncodingId> getEncodingId(String scope, String groupId, @Nullable String subgroupId, VersionInfo version, CompressionType compressionType);

    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String groupId, @Nullable String subgroupId);

    CompletableFuture<List<SchemaWithVersion>> getAllSchemas(String scope, String groupName, @Nullable String subgroupId);

    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema);

    CompletableFuture<Boolean> canRead(String scope, String groupId, @Nullable String subgroupId, VersionInfo writeVersion, VersionInfo readVersion);

    CompletableFuture<Boolean> checkCompatibility(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema);

}

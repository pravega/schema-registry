package io.pravega.schemaregistry.contract;

import java.util.List;

public interface CompatibilityChecker {
    boolean canRead(List<SchemaRegistryContract.SchemaInfo> writerSchemas, SchemaRegistryContract.SchemaInfo readerSchema);
    boolean canBeRead(SchemaRegistryContract.SchemaInfo writerSchema, List<SchemaRegistryContract.SchemaInfo> readerSchemas);
    boolean canMutuallyRead(SchemaRegistryContract.SchemaInfo schema1, SchemaRegistryContract.SchemaInfo schema2);
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

@Data
@Builder
public class NamespaceAndGroup {
    public static final NamespaceAndGroup.Serializer SERIALIZER = new NamespaceAndGroup.Serializer();

    private final String namespace;
    private final String groupId;

    public NamespaceAndGroup(String namespace, String groupId) {
        this.namespace = namespace == null ? "" : namespace;
        this.groupId = groupId;
    }

    @SneakyThrows
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();    
    }
    
    @SneakyThrows
    public static NamespaceAndGroup fromBytes(byte[] bytes) {
        return SERIALIZER.deserialize(bytes);
    }

    private static class NamespaceAndGroupBuilder implements ObjectBuilder<NamespaceAndGroup> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<NamespaceAndGroup, NamespaceAndGroup.NamespaceAndGroupBuilder> {
        @Override
        protected NamespaceAndGroup.NamespaceAndGroupBuilder newBuilder() {
            return NamespaceAndGroup.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(NamespaceAndGroup e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.namespace);
            target.writeUTF(e.groupId);
        }

        private void read00(RevisionDataInput source, NamespaceAndGroup.NamespaceAndGroupBuilder b) throws IOException {
            b.namespace(source.readUTF())
             .groupId(source.readUTF());
        }
    }
}

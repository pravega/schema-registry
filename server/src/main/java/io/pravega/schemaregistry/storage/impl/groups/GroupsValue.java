/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.groups;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

/**
 * Groups listing record in groups table. 
 */
@Data
@Builder
@AllArgsConstructor
public class GroupsValue {
    public static final Serializer SERIALIZER = new Serializer();

    private final String id;
    private final State state;
    
    @SneakyThrows
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    @SneakyThrows
    public static GroupsValue fromBytes(byte[] bytes) {
        return SERIALIZER.deserialize(bytes);
    }
    
    private static class GroupsValueBuilder implements ObjectBuilder<GroupsValue> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<GroupsValue, GroupsValue.GroupsValueBuilder> {
        @Override
        protected GroupsValue.GroupsValueBuilder newBuilder() {
            return GroupsValue.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(GroupsValue e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.id);
            target.writeCompactInt(e.state.ordinal());
        }

        private void read00(RevisionDataInput source, GroupsValue.GroupsValueBuilder b) throws IOException {
            b.id(source.readUTF());
            int ordinal = source.readCompactInt();
            b.state(State.values()[ordinal]);
        }
    }

    public enum State {
        Creating,
        Active,
        Deleting
    }
}

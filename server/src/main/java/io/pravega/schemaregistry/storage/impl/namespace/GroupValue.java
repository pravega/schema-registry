package io.pravega.schemaregistry.storage.impl.namespace;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class GroupValue {
    public static final Serializer SERIALIZER = new Serializer();

    private final String id;
    private final State state;
    
    enum State {
        Creating,
        Active,
        Deleting
    }

    public byte[] toBytes() throws IOException {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    public static GroupValue fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER.deserialize(bytes);
    }
    
    private static class GroupValueBuilder implements ObjectBuilder<GroupValue> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<GroupValue, GroupValue.GroupValueBuilder> {
        @Override
        protected GroupValue.GroupValueBuilder newBuilder() {
            return GroupValue.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(GroupValue e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, GroupValue.GroupValueBuilder b) throws IOException {
        }
    }
}

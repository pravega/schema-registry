package io.pravega.schemaregistry.storage.impl.namespace;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class IdWithState {
    public static final Serializer SERIALIZER = new Serializer();

    private final String id;
    private final State state;
    
    @SneakyThrows
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    @SneakyThrows
    public static IdWithState fromBytes(byte[] bytes) {
        return SERIALIZER.deserialize(bytes);
    }
    
    private static class IdWithStateBuilder implements ObjectBuilder<IdWithState> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<IdWithState, IdWithState.IdWithStateBuilder> {
        @Override
        protected IdWithState.IdWithStateBuilder newBuilder() {
            return IdWithState.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(IdWithState e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, IdWithState.IdWithStateBuilder b) throws IOException {
        }
    }

    public enum State {
        Creating,
        Active,
        Deleting
    }
}

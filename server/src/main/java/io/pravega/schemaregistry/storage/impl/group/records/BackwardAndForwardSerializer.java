/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.BackwardAndForward;
import lombok.SneakyThrows;

import java.io.IOException;

import static io.pravega.schemaregistry.contract.data.BackwardAndForward.Forward;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.Backward;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardAndForwardBuilder;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTransitive;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardPolicy;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.builder;

public class BackwardAndForwardSerializer extends VersionedSerializer.WithBuilder<BackwardAndForward, BackwardAndForwardBuilder> {
    public static final BackwardAndForwardSerializer SERIALIZER = new BackwardAndForwardSerializer();
    private static final BackwardPolicySerializer BACKWARD_POLICY_SERIALIZER = new BackwardPolicySerializer();
    private static final ForwardPolicySerializer FORWARD_POLICY_SERIALIZER = new ForwardPolicySerializer();

    @Override
    protected BackwardAndForwardBuilder newBuilder() {
        return builder();
    }

    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void write00(BackwardAndForward e, RevisionDataOutput target) throws IOException {
        target.writeBoolean(e.getBackwardPolicy() != null);
        if (e.getBackwardPolicy() != null) {
            target.writeArray(BACKWARD_POLICY_SERIALIZER.toBytes(e.getBackwardPolicy()));
        }

        target.writeBoolean(e.getForwardPolicy() != null);
        if (e.getForwardPolicy() != null) {
            target.writeArray(FORWARD_POLICY_SERIALIZER.toBytes(e.getForwardPolicy()));
        }
    }

    private void read00(RevisionDataInput source, BackwardAndForwardBuilder b) throws IOException {
        boolean backwardPolicySpecified = source.readBoolean();
        if (backwardPolicySpecified) {
            b.backwardPolicy(BACKWARD_POLICY_SERIALIZER.fromBytes(source.readArray()));
        }
        boolean forwardPolicySpecified = source.readBoolean();
        if (forwardPolicySpecified) {
            b.forwardPolicy(FORWARD_POLICY_SERIALIZER.fromBytes(source.readArray()));
        }
    }

    static class BackwardPolicySerializer extends VersionedSerializer.MultiType<BackwardPolicy> {

        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(Backward.class, 1, new BackwardSerializer())
                   .serializer(BackwardTill.class, 2, new BackwardTillSerializer())
                   .serializer(BackwardTransitive.class, 3, new BackwardTransitiveSerializer());
        }

        @SneakyThrows(IOException.class)
        public byte[] toBytes(BackwardPolicy value) {
            ByteArraySegment s = serialize(value);
            return s.getCopy();
        }

        @SneakyThrows(IOException.class)
        public BackwardPolicy fromBytes(byte[] buffer) {
            return deserialize(new ByteArraySegment(buffer));
        }
    }

    static class ForwardPolicySerializer extends VersionedSerializer.MultiType<ForwardPolicy> {

        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(Forward.class, 1, new ForwardSerializer())
                   .serializer(ForwardTill.class, 2, new ForwardTillSerializer())
                   .serializer(ForwardTransitive.class, 3, new ForwardTransitiveSerializer());
        }

        @SneakyThrows(IOException.class)
        public byte[] toBytes(ForwardPolicy value) {
            ByteArraySegment s = serialize(value);
            return s.getCopy();
        }

        @SneakyThrows(IOException.class)
        public ForwardPolicy fromBytes(byte[] buffer) {
            return deserialize(new ByteArraySegment(buffer));
        }
    }

    static class BackwardSerializer extends VersionedSerializer.WithBuilder<Backward, Backward.BackwardBuilder> {
        @Override
        protected Backward.BackwardBuilder newBuilder() {
            return Backward.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(Backward e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, Backward.BackwardBuilder b) throws IOException {
        }
    }

    static class BackwardTillSerializer extends VersionedSerializer.WithBuilder<BackwardTill, BackwardTill.BackwardTillBuilder> {
        @Override
        protected BackwardTill.BackwardTillBuilder newBuilder() {
            return BackwardTill.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(BackwardTill e, RevisionDataOutput target) throws IOException {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getVersionInfo());
        }

        private void read00(RevisionDataInput source, BackwardTill.BackwardTillBuilder b) throws IOException {
            b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source));
        }
    }
    
    static class BackwardTransitiveSerializer extends VersionedSerializer.WithBuilder<BackwardTransitive, BackwardTransitive.BackwardTransitiveBuilder> {
        @Override
        protected BackwardTransitive.BackwardTransitiveBuilder newBuilder() {
            return BackwardTransitive.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(BackwardTransitive e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, BackwardTransitive.BackwardTransitiveBuilder b) throws IOException {
        }
    }
   
    static class ForwardSerializer extends VersionedSerializer.WithBuilder<Forward, Forward.ForwardBuilder> {
        @Override
        protected Forward.ForwardBuilder newBuilder() {
            return Forward.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(Forward e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, Forward.ForwardBuilder b) throws IOException {
        }
    }

    static class ForwardTillSerializer extends VersionedSerializer.WithBuilder<ForwardTill, ForwardTill.ForwardTillBuilder> {
        @Override
        protected ForwardTill.ForwardTillBuilder newBuilder() {
            return ForwardTill.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ForwardTill e, RevisionDataOutput target) throws IOException {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getVersionInfo());
        }

        private void read00(RevisionDataInput source, ForwardTill.ForwardTillBuilder b) throws IOException {
            b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source));
        }
    }
    
    static class ForwardTransitiveSerializer extends VersionedSerializer.WithBuilder<ForwardTransitive, ForwardTransitive.ForwardTransitiveBuilder> {
        @Override
        protected ForwardTransitive.ForwardTransitiveBuilder newBuilder() {
            return ForwardTransitive.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ForwardTransitive e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, ForwardTransitive.ForwardTransitiveBuilder b) throws IOException {
        }
    }
}

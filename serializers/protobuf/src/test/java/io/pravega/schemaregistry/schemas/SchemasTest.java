/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.schemas;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.testobjs.generated.ProtobufTest;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SchemasTest {
    @Test
    public void testProtobufSchema() throws IOException {
        ProtobufSchema<ProtobufTest.Message1> sm1 = ProtobufSchema.of(ProtobufTest.Message1.class);
        assertNotNull(sm1.getParser());
        assertNotNull(sm1.getFileDescriptorSet());
        assertEquals(sm1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> bm1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class);
        assertNotNull(bm1.getParser());
        assertNotNull(bm1.getFileDescriptorSet());
        assertEquals(bm1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> bm2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class);
        assertNotNull(bm2.getParser());
        assertNotNull(bm2.getFileDescriptorSet());
        assertEquals(bm2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<DynamicMessage> schema = ProtobufSchema.of(ProtobufTest.Message1.class.getName(), descriptorSet);
        assertNull(schema.getParser());
        assertNotNull(schema.getFileDescriptorSet());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<ProtobufTest.Message1> schema2 = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);
        assertNotNull(schema2.getParser());
        assertNotNull(schema2.getFileDescriptorSet());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> baseSchema1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class, descriptorSet);
        assertNotNull(baseSchema1.getParser());
        assertNotNull(baseSchema1.getFileDescriptorSet());
        assertEquals(baseSchema1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);
        
        ProtobufSchema<GeneratedMessageV3> baseSchema2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class, descriptorSet);
        assertNotNull(baseSchema2.getParser());
        assertNotNull(baseSchema2.getFileDescriptorSet());
        assertEquals(baseSchema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);
    }
}

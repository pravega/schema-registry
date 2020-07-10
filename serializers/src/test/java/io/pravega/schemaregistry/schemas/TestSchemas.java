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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.testobjs.DerivedUser1;
import io.pravega.schemaregistry.testobjs.DerivedUser2;
import io.pravega.schemaregistry.testobjs.SchemaDefinitions;
import io.pravega.schemaregistry.testobjs.User;
import io.pravega.schemaregistry.testobjs.generated.ProtobufTest;
import io.pravega.schemaregistry.testobjs.generated.Test1;
import io.pravega.schemaregistry.testobjs.generated.Test2;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.pravega.schemaregistry.testobjs.SchemaDefinitions.JSON_SCHEMA_STRING;
import static org.junit.Assert.*;

public class TestSchemas {
    @Test
    public void testAvroSchema() {
        AvroSchema<Object> schema = AvroSchema.of(SchemaDefinitions.SCHEMA1);
        assertNotNull(schema.getSchema());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<User> schema2 = AvroSchema.of(User.class);
        assertNotNull(schema2.getSchema());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);
        assertNotNull(schema3.getSchema());
        assertEquals(schema3.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<SpecificRecordBase> schemabase1 = AvroSchema.ofSpecificRecord(Test1.class);
        assertNotNull(schemabase1.getSchema());
        assertEquals(schemabase1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<SpecificRecordBase> schemabase2 = AvroSchema.ofSpecificRecord(Test2.class);
        assertNotNull(schemabase2.getSchema());
        assertEquals(schemabase2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);
    }

    @Test
    public void testProtobufSchema() throws IOException {
        ProtobufSchema<ProtobufTest.Message1> sm1 = ProtobufSchema.of(ProtobufTest.Message1.class);
        assertNotNull(sm1.getParser());
        assertNotNull(sm1.getDescriptorProto());
        assertEquals(sm1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> bm1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class);
        assertNotNull(bm1.getParser());
        assertNotNull(bm1.getDescriptorProto());
        assertEquals(bm1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> bm2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class);
        assertNotNull(bm2.getParser());
        assertNotNull(bm2.getDescriptorProto());
        assertEquals(bm2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<DynamicMessage> schema = ProtobufSchema.of(ProtobufTest.Message1.class.getName(), descriptorSet);
        assertNull(schema.getParser());
        assertNotNull(schema.getDescriptorProto());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<ProtobufTest.Message1> schema2 = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);
        assertNotNull(schema2.getParser());
        assertNotNull(schema2.getDescriptorProto());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);

        ProtobufSchema<GeneratedMessageV3> baseSchema1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class, descriptorSet);
        assertNotNull(baseSchema1.getParser());
        assertNotNull(baseSchema1.getDescriptorProto());
        assertEquals(baseSchema1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);
        
        ProtobufSchema<GeneratedMessageV3> baseSchema2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class, descriptorSet);
        assertNotNull(baseSchema2.getParser());
        assertNotNull(baseSchema2.getDescriptorProto());
        assertEquals(baseSchema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Protobuf);
    }

    @Test
    public void testJsonSchema() throws JsonProcessingException {
        JSONSchema<User> schema = JSONSchema.of(User.class);
        assertNotNull(schema.getSchema());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);

        JSONSchema<Object> schema2 = JSONSchema.of("Person", JSON_SCHEMA_STRING);
        assertNotNull(schema2.getSchema());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
        
        JSONSchema<User> baseSchema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
        assertNotNull(baseSchema1.getSchema());
        assertEquals(baseSchema1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
        JSONSchema<User> baseSchema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);
        assertNotNull(baseSchema2.getSchema());
        assertEquals(baseSchema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
    }
}

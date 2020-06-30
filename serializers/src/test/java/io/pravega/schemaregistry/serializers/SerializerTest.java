/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.testobjs.Address;
import io.pravega.schemaregistry.testobjs.DerivedUser1;
import io.pravega.schemaregistry.testobjs.DerivedUser2;
import io.pravega.schemaregistry.testobjs.SchemaDefinitions;
import io.pravega.schemaregistry.testobjs.generated.ProtobufTest;
import io.pravega.schemaregistry.testobjs.generated.Test1;
import io.pravega.schemaregistry.testobjs.generated.Test2;
import io.pravega.test.common.AssertExtensions;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SerializerTest {
    @Test
    public void testAvroSerializers() {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);

        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        AvroSchema<Test1> schema1 = AvroSchema.of(Test1.class);
        AvroSchema<Test2> schema2 = AvroSchema.of(Test2.class);
        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        VersionInfo versionInfo2 = new VersionInfo("name", 1, 1);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> versionInfo2).when(client).getVersionForSchema(anyString(), eq(schema2.getSchemaInfo()));
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), eq(versionInfo1), any());
        doAnswer(x -> new EncodingId(1)).when(client).getEncodingId(anyString(), eq(versionInfo2), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        AvroSchema<Object> of = AvroSchema.of(SchemaDefinitions.ENUM);
        VersionInfo versionInfo3 = new VersionInfo(of.getSchema().getFullName(), 0, 2);
        doAnswer(x -> versionInfo3).when(client).getVersionForSchema(anyString(), eq(of.getSchemaInfo()));
        doAnswer(x -> new EncodingId(2)).when(client).getEncodingId(anyString(), eq(versionInfo3), any());
        doAnswer(x -> new EncodingInfo(versionInfo3, of.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(2)));

        Serializer<Object> serializerStr = SerializerFactory.avroSerializer(config, of);
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(of.getSchema(), "a");
        ByteBuffer serialized1 = serializerStr.serialize(enumSymbol);

        Serializer<Object> deserializer1 = SerializerFactory.avroDeserializer(config, of);
        Object deserializedEnum = deserializer1.deserialize(serialized1);
        assertEquals(deserializedEnum, enumSymbol);
        
        Serializer<Test1> serializer = SerializerFactory.avroSerializer(config, schema1);
        Test1 test1 = new Test1("name", 1);
        ByteBuffer serialized = serializer.serialize(test1);

        Serializer<Test1> deserializer = SerializerFactory.avroDeserializer(config, schema1);
        Test1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, test1);

        serialized = serializer.serialize(test1);
        Serializer<GenericRecord> genericDeserializer = SerializerFactory.avroGenericDeserializer(config, null);
        GenericRecord genericDeserialized = genericDeserializer.deserialize(serialized);
        assertEquals(genericDeserialized.get("name").toString(), "name");
        assertEquals(genericDeserialized.get("field1"), 1);

        // multi type
        Test2 test2 = new Test2("name", 1, "2");

        AvroSchema<SpecificRecordBase> schema1Base = AvroSchema.ofSpecificRecord(Test1.class);
        AvroSchema<SpecificRecordBase> schema2Base = AvroSchema.ofSpecificRecord(Test2.class);
        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Test1.class, schema1Base);
        map.put(Test2.class, schema2Base);
        Serializer<SpecificRecordBase> multiSerializer = SerializerFactory.avroMultiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(test1);
        Serializer<SpecificRecordBase> multiDeserializer = SerializerFactory.avroMultiTypeDeserializer(config, map);
        SpecificRecordBase deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, test1);

        serialized = multiSerializer.serialize(test2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, test2);

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map2 = new HashMap<>();
        map2.put(Test1.class, schema1Base);
        Serializer<Either<SpecificRecordBase, GenericRecord>> fallbackDeserializer = SerializerFactory.avroTypedOrGenericDeserializer(config, map2);

        serialized = multiSerializer.serialize(test1);
        Either<SpecificRecordBase, GenericRecord> fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isLeft());
        assertEquals(fallback.getLeft(), test1);

        serialized = multiSerializer.serialize(test2);

        fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isRight());
    }

    @Test
    @SneakyThrows
    public void testAvroSerializersReflect() {
        TestClass test1 = new TestClass("name");
        AvroSchema<TestClass> schema1 = AvroSchema.of(TestClass.class);

        SchemaRegistryClient client = mock(SchemaRegistryClient.class);

        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), eq(versionInfo1), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<TestClass> serializer = SerializerFactory.avroSerializer(config, schema1);
        ByteBuffer serialized = serializer.serialize(test1);

        Serializer<TestClass> deserializer = SerializerFactory.avroDeserializer(config, schema1);
        TestClass deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, test1);
    }

    @Test
    public void testProtobufSerializers() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);
        ProtobufSchema<ProtobufTest.Message2> schema1 = ProtobufSchema.of(ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<ProtobufTest.Message3> schema2 = ProtobufSchema.of(ProtobufTest.Message3.class, descriptorSet);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        VersionInfo versionInfo2 = new VersionInfo("name", 1, 1);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> versionInfo2).when(client).getVersionForSchema(anyString(), eq(schema2.getSchemaInfo()));
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), eq(versionInfo1), any());
        doAnswer(x -> new EncodingId(1)).when(client).getEncodingId(anyString(), eq(versionInfo2), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<ProtobufTest.Message2> serializer = SerializerFactory.protobufSerializer(config, schema1);
        ProtobufTest.Message2 message = ProtobufTest.Message2.newBuilder().setName("name").setField1(1).build();
        ByteBuffer serialized = serializer.serialize(message);

        Serializer<ProtobufTest.Message2> deserializer = SerializerFactory.protobufDeserializer(config, schema1);
        ProtobufTest.Message2 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, message);

        serialized = serializer.serialize(message);
        Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(config, null);
        DynamicMessage generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.getAllFields().size(), 2);

        // multi type
        ProtobufTest.Message3 message2 = ProtobufTest.Message3.newBuilder().setName("name").setField1(1).setField2(2).build();

        ProtobufSchema<GeneratedMessageV3> schema1Base = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2Base = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message3.class, descriptorSet);
        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message2.class, schema1Base);
        map.put(ProtobufTest.Message3.class, schema2Base);
        Serializer<GeneratedMessageV3> multiSerializer = SerializerFactory.protobufMultiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(message);
        Serializer<GeneratedMessageV3> multiDeserializer = SerializerFactory.protobufMultiTypeDeserializer(config, map);
        GeneratedMessageV3 deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, message);

        serialized = multiSerializer.serialize(message2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, message2);

        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map2 = new HashMap<>();
        map2.put(ProtobufTest.Message2.class, schema1Base);
        Serializer<Either<GeneratedMessageV3, DynamicMessage>> fallbackDeserializer = SerializerFactory.protobufTypedOrGenericDeserializer(config, map2);
        serialized = multiSerializer.serialize(message);
        Either<GeneratedMessageV3, DynamicMessage> fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isLeft());
        assertEquals(fallback.getLeft(), message);

        serialized = multiSerializer.serialize(message2);

        fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isRight());
    }

    @Test
    public void testJsonSerializers() {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        JSONSchema<DerivedUser1> schema1 = JSONSchema.of(DerivedUser1.class);
        JSONSchema<DerivedUser2> schema2 = JSONSchema.of(DerivedUser2.class);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        VersionInfo versionInfo2 = new VersionInfo("name", 1, 1);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> versionInfo2).when(client).getVersionForSchema(anyString(), eq(schema2.getSchemaInfo()));
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), eq(versionInfo1), any());
        doAnswer(x -> new EncodingId(1)).when(client).getEncodingId(anyString(), eq(versionInfo2), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<DerivedUser1> serializer = SerializerFactory.jsonSerializer(config, schema1);
        DerivedUser1 user1 = new DerivedUser1("user", new Address("street", "city"), 2, "user1");
        ByteBuffer serialized = serializer.serialize(user1);

        Serializer<DerivedUser1> deserializer = SerializerFactory.jsonDeserializer(config, schema1);
        DerivedUser1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, user1);

        serialized = serializer.serialize(user1);
        Serializer<JsonGenericObject> genericDeserializer = SerializerFactory.jsonGenericDeserializer(config);
        JsonGenericObject generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.getJsonSchema(), schema1.getSchema());
        assertEquals(generic.getObject().size(), 4);

        serialized = serializer.serialize(user1);
        Serializer<String> stringDeserializer = SerializerFactory.jsonStringDeserializer(config);
        String str = stringDeserializer.deserialize(serialized);
        assertFalse(Strings.isNullOrEmpty(str));

        // multi type
        DerivedUser2 user2 = new DerivedUser2("user", new Address("street", "city"), 2, "user2");

        JSONSchema<Object> schema1Base = JSONSchema.ofBaseType(DerivedUser1.class, Object.class);
        JSONSchema<Object> schema2Base = JSONSchema.ofBaseType(DerivedUser2.class, Object.class);
        Map<Class<? extends Object>, JSONSchema<Object>> map = new HashMap<>();
        map.put(DerivedUser1.class, schema1Base);
        map.put(DerivedUser2.class, schema2Base);
        Serializer<Object> multiSerializer = SerializerFactory.jsonMultiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(user1);
        Serializer<Object> multiDeserializer = SerializerFactory.jsonMultiTypeDeserializer(config, map);
        Object deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, user1);

        serialized = multiSerializer.serialize(user2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, user2);

        Map<Class<? extends Object>, JSONSchema<Object>> map2 = new HashMap<>();
        map2.put(DerivedUser1.class, schema1Base);
        Serializer<Either<Object, JsonGenericObject>> fallbackDeserializer = SerializerFactory.jsonTypedOrGenericDeserializer(config, map2);
        serialized = multiSerializer.serialize(user1);
        Either<Object, JsonGenericObject> fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isLeft());
        assertEquals(fallback.getLeft(), user1);

        serialized = multiSerializer.serialize(user2);

        fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isRight());
    }

    @Test
    public void testMultiformatDeserializers() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        AvroSchema<Test1> schema1 = AvroSchema.of(Test1.class);
        ProtobufSchema<ProtobufTest.Message2> schema2 = ProtobufSchema.of(ProtobufTest.Message2.class, descriptorSet);
        JSONSchema<DerivedUser1> schema3 = JSONSchema.of(DerivedUser1.class);

        VersionInfo versionInfo1 = new VersionInfo("avro", 0, 0);
        VersionInfo versionInfo2 = new VersionInfo("proto", 1, 1);
        VersionInfo versionInfo3 = new VersionInfo("json", 2, 2);

        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> versionInfo2).when(client).getVersionForSchema(anyString(), eq(schema2.getSchemaInfo()));
        doAnswer(x -> versionInfo3).when(client).getVersionForSchema(anyString(), eq(schema3.getSchemaInfo()));
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), eq(versionInfo1), any());
        doAnswer(x -> new EncodingId(1)).when(client).getEncodingId(anyString(), eq(versionInfo2), any());
        doAnswer(x -> new EncodingId(2)).when(client).getEncodingId(anyString(), eq(versionInfo3), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> new EncodingInfo(versionInfo3, schema3.getSchemaInfo(), CodecFactory.NONE)).when(client).getEncodingInfo(anyString(), eq(new EncodingId(2)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<Test1> avroSerializer = SerializerFactory.avroSerializer(config, schema1);
        Test1 test1 = new Test1("name", 1);
        ByteBuffer serializedAvro = avroSerializer.serialize(test1);

        Serializer<ProtobufTest.Message2> protobufSerializer = SerializerFactory.protobufSerializer(config, schema2);
        ProtobufTest.Message2 message = ProtobufTest.Message2.newBuilder().setName("name").setField1(1).build();
        ByteBuffer serializedProto = protobufSerializer.serialize(message);

        Serializer<DerivedUser1> jsonSerializer = SerializerFactory.jsonSerializer(config, schema3);
        DerivedUser1 user1 = new DerivedUser1("user", new Address("street", "city"), 2, "user1");
        ByteBuffer serializedJson = jsonSerializer.serialize(user1);

        Serializer<Object> deserializer = SerializerFactory.multiFormatGenericDeserializer(config);
        Object deserialized = deserializer.deserialize(serializedAvro);
        assertTrue(deserialized instanceof GenericRecord);
        deserialized = deserializer.deserialize(serializedProto);
        assertTrue(deserialized instanceof DynamicMessage);
        deserialized = deserializer.deserialize(serializedJson);
        assertTrue(deserialized instanceof JsonGenericObject);

        Serializer<String> jsonStringDeserializer = SerializerFactory.deserializeAsJsonString(config);
        serializedAvro.position(0);
        String jsonString = jsonStringDeserializer.deserialize(serializedAvro);
        assertNotNull(jsonString);
        serializedProto.position(0);
        jsonString = jsonStringDeserializer.deserialize(serializedProto);
        assertNotNull(jsonString);
        serializedJson.position(0);
        jsonString = jsonStringDeserializer.deserialize(serializedJson);
        assertNotNull(jsonString);
    }

    @Test
    public void testNoEncodingProto() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);
        ProtobufSchema<ProtobufTest.Message2> schema1 = ProtobufSchema.of(ProtobufTest.Message2.class, descriptorSet);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any)
                                     .properties(ImmutableMap.of(SerializerFactory.ENCODE, Boolean.toString(false))).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> new SchemaWithVersion(schema1.getSchemaInfo(), versionInfo1)).when(client).getLatestSchemaVersion(anyString(), any());
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<ProtobufTest.Message2> serializer = SerializerFactory.protobufSerializer(config, schema1);
        verify(client, never()).getEncodingId(anyString(), any(), any());
        
        ProtobufTest.Message2 message = ProtobufTest.Message2.newBuilder().setName("name").setField1(1).build();
        ByteBuffer serialized = serializer.serialize(message);

        Serializer<ProtobufTest.Message2> deserializer = SerializerFactory.protobufDeserializer(config, schema1);
        verify(client, never()).getEncodingInfo(anyString(), any());

        ProtobufTest.Message2 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, message);

        serialized = serializer.serialize(message);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> SerializerFactory.protobufGenericDeserializer(config, null));

        SchemaInfo latestSchema = client.getLatestSchemaVersion("groupId", null).getSchemaInfo();
        ProtobufSchema<DynamicMessage> schemaDynamic = ProtobufSchema.of(latestSchema.getType(), descriptorSet);
        Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(config, schemaDynamic);
        
        DynamicMessage generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.getAllFields().size(), 2);
    }
    
    @Test
    public void testNoEncodingJson() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        JSONSchema<DerivedUser1> schema1 = JSONSchema.of(DerivedUser1.class);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any)
                .properties(ImmutableMap.of(SerializerFactory.ENCODE, Boolean.toString(false))).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> new SchemaWithVersion(schema1.getSchemaInfo(), versionInfo1)).when(client).getLatestSchemaVersion(anyString(), any());
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<DerivedUser1> serializer = SerializerFactory.jsonSerializer(config, schema1);
        verify(client, never()).getEncodingId(anyString(), any(), any());
        DerivedUser1 user1 = new DerivedUser1("user", new Address("street", "city"), 2, "user1");
        ByteBuffer serialized = serializer.serialize(user1);

        Serializer<DerivedUser1> deserializer = SerializerFactory.jsonDeserializer(config, schema1);
        verify(client, never()).getEncodingInfo(anyString(), any());
        DerivedUser1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, user1);
        
        serialized = serializer.serialize(user1);

        Serializer<JsonGenericObject> genericDeserializer = SerializerFactory.jsonGenericDeserializer(config);

        JsonGenericObject generic = genericDeserializer.deserialize(serialized);
        assertNotNull(generic.getObject());
        assertNull(generic.getJsonSchema());
    }

    @Data
    @NoArgsConstructor
    public static class TestClass {
        private String test;

        public TestClass(String test) {
            this.test = test;
        }
    }
}

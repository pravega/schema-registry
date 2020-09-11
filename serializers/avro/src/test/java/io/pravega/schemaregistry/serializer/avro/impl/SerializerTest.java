/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.avro.impl;

import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializer.avro.testobjs.SchemaDefinitions;
import io.pravega.schemaregistry.serializer.avro.testobjs.generated.Test1;
import io.pravega.schemaregistry.serializer.avro.testobjs.generated.Test2;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        AvroSchema<Object> of = AvroSchema.of(SchemaDefinitions.ENUM);
        VersionInfo versionInfo3 = new VersionInfo(of.getSchema().getFullName(), 0, 2);
        doAnswer(x -> versionInfo3).when(client).getVersionForSchema(anyString(), eq(of.getSchemaInfo()));
        doAnswer(x -> new EncodingId(2)).when(client).getEncodingId(anyString(), eq(versionInfo3), any());
        doAnswer(x -> new EncodingInfo(versionInfo3, of.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(2)));

        Serializer<Object> serializerStr = AvroSerializerFactory.serializer(config, of);
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(of.getSchema(), "a");
        ByteBuffer serialized1 = serializerStr.serialize(enumSymbol);

        Serializer<Object> deserializer1 = AvroSerializerFactory.deserializer(config, of);
        Object deserializedEnum = deserializer1.deserialize(serialized1);
        assertEquals(deserializedEnum, enumSymbol);
        
        Serializer<Test1> serializer = AvroSerializerFactory.serializer(config, schema1);
        Test1 test1 = new Test1("name", 1);
        ByteBuffer serialized = serializer.serialize(test1);

        Serializer<Test1> deserializer = AvroSerializerFactory.deserializer(config, schema1);
        Test1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, test1);

        serialized = serializer.serialize(test1);
        Serializer<Object> genericDeserializer = AvroSerializerFactory.genericDeserializer(config, null);
        Object genericDeserialized = genericDeserializer.deserialize(serialized);
        assertTrue(genericDeserialized instanceof GenericRecord);
        assertEquals(((GenericRecord) genericDeserialized).get("name").toString(), "name");
        assertEquals(((GenericRecord) genericDeserialized).get("field1"), 1);

        // multi type
        Test2 test2 = new Test2("name", 1, "2");

        AvroSchema<SpecificRecordBase> schema1Base = AvroSchema.ofSpecificRecord(Test1.class);
        AvroSchema<SpecificRecordBase> schema2Base = AvroSchema.ofSpecificRecord(Test2.class);
        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Test1.class, schema1Base);
        map.put(Test2.class, schema2Base);
        Serializer<SpecificRecordBase> multiSerializer = AvroSerializerFactory.multiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(test1);
        Serializer<SpecificRecordBase> multiDeserializer = AvroSerializerFactory.multiTypeDeserializer(config, map);
        SpecificRecordBase deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, test1);

        serialized = multiSerializer.serialize(test2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, test2);

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map2 = new HashMap<>();
        map2.put(Test1.class, schema1Base);
        Serializer<Either<SpecificRecordBase, Object>> fallbackDeserializer = AvroSerializerFactory.typedOrGenericDeserializer(config, map2);

        serialized = multiSerializer.serialize(test1);
        Either<SpecificRecordBase, Object> fallback = fallbackDeserializer.deserialize(serialized);
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
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<TestClass> serializer = AvroSerializerFactory.serializer(config, schema1);
        ByteBuffer serialized = serializer.serialize(test1);

        Serializer<TestClass> deserializer = AvroSerializerFactory.deserializer(config, schema1);
        TestClass deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, test1);
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

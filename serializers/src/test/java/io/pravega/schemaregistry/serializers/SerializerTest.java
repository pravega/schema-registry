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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.avro.testobjs.generated.Test1;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.protobuf.generated.ProtobufTest;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializer.shared.testobjs.Address;
import io.pravega.schemaregistry.serializer.shared.testobjs.DerivedUser1;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SerializerTest {
    @Test
    public void testMultiformatDeserializers() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);

        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId").build();
        AvroSchema<Test1> schema1 = AvroSchema.of(Test1.class);
        ProtobufSchema<ProtobufTest.Message2> schema2 = ProtobufSchema.of(ProtobufTest.Message2.class);
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
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> new EncodingInfo(versionInfo3, schema3.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(2)));
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

        Serializer<Object> deserializer = SerializerFactory.genericDeserializer(config);
        Object deserialized = deserializer.deserialize(serializedAvro);
        assertTrue(deserialized instanceof GenericRecord);
        deserialized = deserializer.deserialize(serializedProto);
        assertTrue(deserialized instanceof DynamicMessage);
        deserialized = deserializer.deserialize(serializedJson);
        assertTrue(deserialized instanceof JsonNode);

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
}

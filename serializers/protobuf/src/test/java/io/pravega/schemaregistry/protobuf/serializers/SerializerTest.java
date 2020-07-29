/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.protobuf.serializers;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.shared.codec.Codecs;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.protobuf.testobjs.generated.ProtobufTest;
import io.pravega.schemaregistry.shared.serializers.SerializerConfig;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SerializerTest {
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
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<ProtobufTest.Message2> serializer = ProtobufSerializerFactory.serializer(config, schema1);
        ProtobufTest.Message2 message = ProtobufTest.Message2.newBuilder().setName("name").setField1(1).build();
        ByteBuffer serialized = serializer.serialize(message);

        Serializer<ProtobufTest.Message2> deserializer = ProtobufSerializerFactory.deserializer(config, schema1);
        ProtobufTest.Message2 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, message);

        serialized = serializer.serialize(message);
        Serializer<DynamicMessage> genericDeserializer = ProtobufSerializerFactory.genericDeserializer(config, null);
        DynamicMessage generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.getAllFields().size(), 2);

        // multi type
        ProtobufTest.Message3 message2 = ProtobufTest.Message3.newBuilder().setName("name").setField1(1).setField2(2).build();

        ProtobufSchema<GeneratedMessageV3> schema1Base = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2Base = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message3.class, descriptorSet);
        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message2.class, schema1Base);
        map.put(ProtobufTest.Message3.class, schema2Base);
        Serializer<GeneratedMessageV3> multiSerializer = ProtobufSerializerFactory.multiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(message);
        Serializer<GeneratedMessageV3> multiDeserializer = ProtobufSerializerFactory.multiTypeDeserializer(config, map);
        GeneratedMessageV3 deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, message);

        serialized = multiSerializer.serialize(message2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, message2);

        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map2 = new HashMap<>();
        map2.put(ProtobufTest.Message2.class, schema1Base);
        Serializer<Either<GeneratedMessageV3, DynamicMessage>> fallbackDeserializer = ProtobufSerializerFactory.typedOrGenericDeserializer(config, map2);
        serialized = multiSerializer.serialize(message);
        Either<GeneratedMessageV3, DynamicMessage> fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isLeft());
        assertEquals(fallback.getLeft(), message);

        serialized = multiSerializer.serialize(message2);

        fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isRight());
    }

    @Test
    public void testNoEncodingProto() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId")
                                                  .writeEncodingHeader(false).build();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);
        ProtobufSchema<ProtobufTest.Message2> schema1 = ProtobufSchema.of(ProtobufTest.Message2.class, descriptorSet);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any)
                                     .properties(ImmutableMap.of()).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> new SchemaWithVersion(schema1.getSchemaInfo(), versionInfo1)).when(client).getLatestSchemaVersion(anyString(), any());
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<ProtobufTest.Message2> serializer = ProtobufSerializerFactory.serializer(config, schema1);
        verify(client, never()).getEncodingId(anyString(), any(), any());
        
        ProtobufTest.Message2 message = ProtobufTest.Message2.newBuilder().setName("name").setField1(1).build();
        ByteBuffer serialized = serializer.serialize(message);

        Serializer<ProtobufTest.Message2> deserializer = ProtobufSerializerFactory.deserializer(config, schema1);
        verify(client, never()).getEncodingInfo(anyString(), any());

        ProtobufTest.Message2 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, message);

        serialized = serializer.serialize(message);
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ProtobufSerializerFactory.genericDeserializer(config, null));

        SchemaInfo latestSchema = client.getLatestSchemaVersion("groupId", null).getSchemaInfo();
        ProtobufSchema<DynamicMessage> schemaDynamic = ProtobufSchema.of(latestSchema.getType(), descriptorSet);
        Serializer<DynamicMessage> genericDeserializer = ProtobufSerializerFactory.genericDeserializer(config, schemaDynamic);
        
        DynamicMessage generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.getAllFields().size(), 2);
    }
}

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.Codecs;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.testobjs.Address;
import io.pravega.schemaregistry.testobjs.DerivedUser1;
import io.pravega.schemaregistry.testobjs.DerivedUser2;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SerializerTest {
    @Test
    public void testJsonSerializers() throws JsonProcessingException {
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
        doAnswer(x -> new EncodingInfo(versionInfo1, schema1.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        doAnswer(x -> new EncodingInfo(versionInfo2, schema2.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(1)));
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<DerivedUser1> serializer = JsonSerializerFactory.serializer(config, schema1);
        DerivedUser1 user1 = new DerivedUser1("user", new Address("street", "city"), 2, "user1");
        ByteBuffer serialized = serializer.serialize(user1);

        Serializer<DerivedUser1> deserializer = JsonSerializerFactory.deserializer(config, schema1);
        DerivedUser1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, user1);

        serialized = serializer.serialize(user1);
        Serializer<JsonNode> genericDeserializer = JsonSerializerFactory.genericDeserializer(config);
        JsonNode generic = genericDeserializer.deserialize(serialized);
        assertEquals(generic.size(), 4);

        serialized = serializer.serialize(user1);
        Serializer<String> stringDeserializer = JsonSerializerFactory.deserializeAsString(config);
        String str = stringDeserializer.deserialize(serialized);
        assertFalse(Strings.isNullOrEmpty(str));

        String schemaString = "{\"type\": \"object\",\"title\": \"The external data schema\",\"properties\": {\"content\": {\"type\": \"string\"}}}";

        JSONSchema<HashMap> myData = JSONSchema.of("MyData", schemaString, HashMap.class);
        VersionInfo versionInfo3 = new VersionInfo("myData", 0, 2);
        doAnswer(x -> versionInfo3).when(client).getVersionForSchema(anyString(), eq(myData.getSchemaInfo()));
        doAnswer(x -> new EncodingId(2)).when(client).getEncodingId(anyString(), eq(versionInfo3), any());
        doAnswer(x -> new EncodingInfo(versionInfo3, myData.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(2)));

        Serializer<HashMap> serializer2 = JsonSerializerFactory.serializer(config, myData);
        HashMap<String, String> jsonObject = new HashMap<>();
        jsonObject.put("content", "mxx");
        
        ByteBuffer s = serializer2.serialize(jsonObject);
        str = stringDeserializer.deserialize(s);
        
        String stringSchema = new ObjectMapper().writeValueAsString(JsonSchema.minimalForFormat(JsonFormatTypes.STRING));

        JSONSchema<String> strSchema = JSONSchema.of("string", stringSchema, String.class);
        VersionInfo versionInfo4 = new VersionInfo("myData", 0, 3);
        doAnswer(x -> versionInfo4).when(client).getVersionForSchema(anyString(), eq(strSchema.getSchemaInfo()));
        doAnswer(x -> new EncodingId(3)).when(client).getEncodingId(anyString(), eq(versionInfo4), any());
        doAnswer(x -> new EncodingInfo(versionInfo4, strSchema.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(3)));

        Serializer<String> serializer3 = JsonSerializerFactory.serializer(config, strSchema);
        Serializer<String> deserializer3 = JsonSerializerFactory.deserializer(config, strSchema);
        Serializer<JsonNode> generic3 = JsonSerializerFactory.genericDeserializer(config);
        String string = "a";
        s = serializer3.serialize(string);
        Object x = deserializer3.deserialize(s);
        assertNotNull(x);
        assertEquals(x, string);
        s = serializer3.serialize(string);
        Object jsonNode = generic3.deserialize(s);
        assertTrue(jsonNode instanceof TextNode);
        assertEquals(((TextNode) jsonNode).textValue(), string);
        // multi type
        DerivedUser2 user2 = new DerivedUser2("user", new Address("street", "city"), 2, "user2");

        JSONSchema<Object> schema1Base = JSONSchema.ofBaseType(DerivedUser1.class, Object.class);
        JSONSchema<Object> schema2Base = JSONSchema.ofBaseType(DerivedUser2.class, Object.class);
        Map<Class<?>, JSONSchema<Object>> map = new HashMap<>();
        map.put(DerivedUser1.class, schema1Base);
        map.put(DerivedUser2.class, schema2Base);
        Serializer<Object> multiSerializer = JsonSerializerFactory.multiTypeSerializer(config, map);
        serialized = multiSerializer.serialize(user1);
        Serializer<Object> multiDeserializer = JsonSerializerFactory.multiTypeDeserializer(config, map);
        Object deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, user1);

        serialized = multiSerializer.serialize(user2);
        deserialized2 = multiDeserializer.deserialize(serialized);
        assertEquals(deserialized2, user2);

        Map<Class<?>, JSONSchema<Object>> map2 = new HashMap<>();
        map2.put(DerivedUser1.class, schema1Base);
        Serializer<Either<Object, JsonNode>> fallbackDeserializer = JsonSerializerFactory.typedOrGenericDeserializer(config, map2);
        serialized = multiSerializer.serialize(user1);
        Either<Object, JsonNode> fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isLeft());
        assertEquals(fallback.getLeft(), user1);

        serialized = multiSerializer.serialize(user2);

        fallback = fallbackDeserializer.deserialize(serialized);
        assertTrue(fallback.isRight());
    }

    @Test
    public void testNoEncodingJson() throws IOException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        SerializerConfig config = SerializerConfig.builder().registryClient(client).groupId("groupId")
                                                  .writeEncodingHeader(false).build();
        JSONSchema<DerivedUser1> schema1 = JSONSchema.of(DerivedUser1.class);

        VersionInfo versionInfo1 = new VersionInfo("name", 0, 0);
        doAnswer(x -> GroupProperties.builder().serializationFormat(SerializationFormat.Any)
                .properties(ImmutableMap.of()).build())
                .when(client).getGroupProperties(anyString());
        doAnswer(x -> versionInfo1).when(client).getVersionForSchema(anyString(), eq(schema1.getSchemaInfo()));
        doAnswer(x -> new SchemaWithVersion(schema1.getSchemaInfo(), versionInfo1)).when(client).getLatestSchemaVersion(anyString(), any());
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());

        Serializer<DerivedUser1> serializer = JsonSerializerFactory.serializer(config, schema1);
        verify(client, never()).getEncodingId(anyString(), any(), any());
        DerivedUser1 user1 = new DerivedUser1("user", new Address("street", "city"), 2, "user1");
        ByteBuffer serialized = serializer.serialize(user1);

        Serializer<DerivedUser1> deserializer = JsonSerializerFactory.deserializer(config, schema1);
        verify(client, never()).getEncodingInfo(anyString(), any());
        DerivedUser1 deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized, user1);
        
        serialized = serializer.serialize(user1);

        Serializer<JsonNode> genericDeserializer = JsonSerializerFactory.genericDeserializer(config);

        JsonNode generic = genericDeserializer.deserialize(serialized);
        assertNotNull(generic);
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

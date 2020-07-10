/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.serializationformats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.demo.objects.Address;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser1;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser2;
import io.pravega.schemaregistry.samples.demo.objects.User;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.serializers.WithSchema;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample class that demonstrates how to use Json Serializers and Deserializers provided by Schema registry's 
 * {@link SerializerFactory}.
 * This class has multiple deserialization options 
 * 1. Deserialize into java class (schema on read).
 * 2. Deserialize into {@link Map}. No schema. 
 * 3. Deserialize into {@link Map} while retrieving writer schema. 
 * 4. Multiplexed Deserializer that deserializes data into one of java objects based on {@link SchemaInfo#type}.
 */
@Slf4j
public class JsonDemo {
    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;

    public JsonDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:9092")).build();
        client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        id = Long.toString(System.currentTimeMillis());
    }
    
    public static void main(String[] args) throws Exception {
        JsonDemo demo = new JsonDemo();
        
        demo.testJson(true);
        demo.testJson(false);
        demo.testJsonMultiplexed();

        System.exit(0);
    }
    
    private void testJson(boolean encodeHeaders) throws JsonProcessingException {
        if (encodeHeaders) {
            System.out.println("testing json WITH headers encoded in the stream payload");
        } else {
            System.out.println("testing json WITHOUT headers encoded in the stream payload. " +
                    "This will use ");
        }
        
        // create stream
        String scope = "scope" + id;
        String stream = "json" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Scheme.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Json;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    false, ImmutableMap.of()));

            JSONSchema<DerivedUser2> schema = JSONSchema.of(DerivedUser2.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .writeEncodingHeader(encodeHeaders)
                                                                .build();
            // region writer
            Serializer<DerivedUser2> serializer = SerializerFactory.jsonSerializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<DerivedUser2> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new DerivedUser2("name", new Address("street", "city"), 30, "user2"));

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String readerGroupName = "rg" + stream;
                readerGroupManager.createReaderGroup(readerGroupName,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<DerivedUser2> deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);

                EventStreamReader<DerivedUser2> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

                EventRead<DerivedUser2> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;

                // endregion

                // region generic read
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<WithSchema<JsonNode>> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

                EventStreamReader<WithSchema<JsonNode>> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<WithSchema<JsonNode>> event2 = reader2.readNextEvent(1000);
                assert event2.getEvent() != null;
                WithSchema<JsonNode> obj = event2.getEvent();

                JsonSchema jsonSchema = obj.getJsonSchema();
                if (encodeHeaders) {
                    assert jsonSchema != null;
                } else {
                    assert jsonSchema == null;
                }
                // endregion
            }
        }
    }

    private void testJsonMultiplexed() throws JsonProcessingException {
        // create stream
        String scope = "scope" + id;
        String stream = "jsonmultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Scheme.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Json;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    true));

            JSONSchema<User> schema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
            JSONSchema<User> schema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Map<Class<? extends User>, JSONSchema<User>> map = new HashMap<>();
            map.put(DerivedUser1.class, schema1);
            map.put(DerivedUser2.class, schema2);
            Serializer<User> serializer = SerializerFactory.jsonMultiTypeSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<User> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new DerivedUser2());
            writer.writeEvent(new DerivedUser1());

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream;
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<User> deserializer = SerializerFactory.jsonMultiTypeDeserializer(serializerConfig, map);

                EventStreamReader<User> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<User> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                assert event.getEvent() instanceof DerivedUser2;
                event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                assert event.getEvent() instanceof DerivedUser1;
                // endregion

                // region read into writer schema
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<WithSchema<JsonNode>> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

                EventStreamReader<WithSchema<JsonNode>> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<WithSchema<JsonNode>> genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = "rg3" + stream;
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends User>, JSONSchema<User>> map2 = new HashMap<>();
                // add only one schema
                map2.put(DerivedUser1.class, schema1);

                Serializer<Either<User, WithSchema<JsonNode>>> eitherDeserializer =
                        SerializerFactory.jsonTypedOrGenericDeserializer(serializerConfig, map2);

                EventStreamReader<Either<User, WithSchema<JsonNode>>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<User, WithSchema<JsonNode>>> e1 = reader3.readNextEvent(1000);
                assert e1.getEvent() != null;
                assert e1.getEvent().isRight();
                
                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof DerivedUser1;
                //endregion
            }
        }
    }
}
/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.json;

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
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.serializers.JSonGenericObject;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.demo.objects.Address;
import io.pravega.schemaregistry.test.integrationtest.demo.objects.DerivedUser1;
import io.pravega.schemaregistry.test.integrationtest.demo.objects.DerivedUser2;
import io.pravega.schemaregistry.test.integrationtest.demo.objects.User;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonDemo {
    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;

    public JsonDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create("http://localhost:9092"));
        client = RegistryClientFactory.createRegistryClient(config);
        id = Long.toString(System.currentTimeMillis());
    }
    
    public static void main(String[] args) throws Exception {
        JsonDemo demo = new JsonDemo();
        
        demo.testJson(true);
        demo.testJson(false);
        demo.testJsonMultiplexed();

        System.exit(0);
    }
    
    private void testJson(boolean encodeHeaders) {
        // create stream
        String scope = "scope" + id;
        String stream = "json" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SchemaType schemaType = SchemaType.Json;
            client.addGroup(groupId, schemaType,
                    SchemaValidationRules.of(Compatibility.allowAny()),
                    false, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(encodeHeaders)));

            JSONSchema<DerivedUser2> schema = JSONSchema.of(DerivedUser2.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .autoRegisterSchema(true)
                                                                .registryConfigOrClient(Either.right(client))
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
                        ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<DerivedUser2> deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);

                EventStreamReader<DerivedUser2> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

                EventRead<DerivedUser2> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;

                // endregion

                // region generic read
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<JSonGenericObject> genericDeserializer = SerializerFactory.genericJsonDeserializer(serializerConfig);

                EventStreamReader<JSonGenericObject> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<JSonGenericObject> event2 = reader2.readNextEvent(1000);
                assert event2.getEvent() != null;
                JSonGenericObject obj = event2.getEvent();

                com.fasterxml.jackson.module.jsonSchema.JsonSchema jsonSchema = obj.getJsonSchema();
                if (encodeHeaders) {
                    assert jsonSchema != null;
                } else {
                    assert jsonSchema == null;
                }
                // endregion
            }
        }
    }

    private void testJsonMultiplexed() {
        // create stream
        String scope = "scope" + id;
        String stream = "jsonmultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SchemaType schemaType = SchemaType.Json;
            client.addGroup(groupId, schemaType,
                    SchemaValidationRules.of(Compatibility.allowAny()),
                    true, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(true)));

            JSONSchema<User> schema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
            JSONSchema<User> schema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .autoRegisterSchema(true)
                                                                .registryConfigOrClient(Either.right(client))
                                                                .build();
            // region writer
            Map<Class<? extends User>, JSONSchema<User>> map = new HashMap<>();
            map.put(DerivedUser1.class, schema1);
            map.put(DerivedUser2.class, schema2);
            Serializer<User> serializer = SerializerFactory.multiTypedJsonSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<User> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new DerivedUser2());
            writer.writeEvent(new DerivedUser1());

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream;
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<User> deserializer = SerializerFactory.multiTypedJsonDeserializer(serializerConfig, map);

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
                        ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<JSonGenericObject> genericDeserializer = SerializerFactory.genericJsonDeserializer(serializerConfig);

                EventStreamReader<JSonGenericObject> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<JSonGenericObject> genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                genEvent = reader2.readNextEvent(1000);
                assert genEvent.getEvent() != null;
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = "rg3" + stream;
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends User>, JSONSchema<User>> map2 = new HashMap<>();
                // add only one schema
                map2.put(DerivedUser1.class, schema1);

                Serializer<Either<User, JSonGenericObject>> eitherDeserializer =
                        SerializerFactory.typedOrGenericJsonDeserializer(serializerConfig, map2);

                EventStreamReader<Either<User, JSonGenericObject>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<User, JSonGenericObject>> e1 = reader3.readNextEvent(1000);
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


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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
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
import io.pravega.schemaregistry.samples.generated.ProtobufTest;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample class that demonstrates how to use Json Serializers and Deserializers provided by Schema registry's 
 * {@link SerializerFactory}.
 * Protobuf has multiple deserialization options 
 * 1. Deserialize into protobuf generated java class (schema on read).
 * 2. Deserialize into {@link DynamicMessage} using user supplied schema (schema on read). 
 * 3. Deserialize into {@link DynamicMessage} while retrieving writer schema. 
 * 4. Multiplexed Deserializer that deserializes data into one of java objects based on {@link SchemaInfo#type}.
 */
@Slf4j
public class ProtobufDemo {
    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;

    public ProtobufDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:9092")).build();
        client = SchemaRegistryClientFactory.createRegistryClient(config);
        id = Long.toString(System.currentTimeMillis());
    }
    
    public static void main(String[] args) throws Exception {
        ProtobufDemo demo = new ProtobufDemo();

        demo.testProtobuf(true);
        demo.testProtobuf(false);
        demo.testProtobufMultiplexed();

        System.exit(0);
    }
    
    private void testProtobuf(boolean encodeHeaders) throws IOException {
        // create stream
        String scope = "scope" + id;
        String stream = "protobuf" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Protobuf;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    false, ImmutableMap.of(SerializerFactory.ENCODE, Boolean.toString(encodeHeaders))));

            Path path = Paths.get("samples/resources/proto/protobufTest.pb");
            byte[] schemaBytes = Files.readAllBytes(path);
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

            ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Serializer<ProtobufTest.Message1> serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<ProtobufTest.Message1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build()).join();

            // endregion

            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                // region read into specific schema
                String readerGroupName = "rg" + stream;
                readerGroupManager.createReaderGroup(readerGroupName,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<ProtobufTest.Message1> deserializer = SerializerFactory.protobufDeserializer(serializerConfig, schema);

                EventStreamReader<ProtobufTest.Message1> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

                EventRead<ProtobufTest.Message1> event = reader.readNextEvent(1000);
                assert null != event.getEvent();

                // endregion

                if (encodeHeaders) {
                    // region generic read
                    // 1. try without passing the schema. writer schema will be used to read
                    String rg2 = "rg2" + stream;
                    readerGroupManager.createReaderGroup(rg2,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, null);

                    EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                    EventRead<DynamicMessage> event2 = reader2.readNextEvent(1000);
                    assert null != event2.getEvent();

                    // 2. try with passing the schema. reader schema will be used to read
                    String rg3 = "rg3" + encodeHeaders;
                    readerGroupManager.createReaderGroup(rg3,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    ProtobufSchema<DynamicMessage> schema2 = ProtobufSchema.of(ProtobufTest.Message1.getDescriptor().getName(), descriptorSet);
                    genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, schema2);

                    reader2 = clientFactory.createReader("r1", rg3, genericDeserializer, ReaderConfig.builder().build());

                    event2 = reader2.readNextEvent(1000);
                    assert null != event2.getEvent();
                }
                // endregion
            }
        }
    }
    
    private void testProtobufMultiplexed() throws IOException {
        // create stream
        String scope = "scope" + id;
        String stream = "protomultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Protobuf;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.allowAny(),
                    true));

            Path path = Paths.get("samples/resources/proto/protobufTest.pb");
            byte[] schemaBytes = Files.readAllBytes(path);
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

            ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.ofBaseType(ProtobufTest.Message1.class, descriptorSet);
            ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.ofBaseType(ProtobufTest.Message2.class, descriptorSet);
            ProtobufSchema<GeneratedMessageV3> schema3 = ProtobufSchema.ofBaseType(ProtobufTest.Message3.class, descriptorSet);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
            map.put(ProtobufTest.Message1.class, schema1);
            map.put(ProtobufTest.Message2.class, schema2);
            map.put(ProtobufTest.Message3.class, schema3);
            Serializer<GeneratedMessageV3> serializer = SerializerFactory.protobufMultiTypeSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<GeneratedMessageV3> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build()).join();
            writer.writeEvent(ProtobufTest.Message2.newBuilder().setName("test").setField1(0).build()).join();
            writer.writeEvent(ProtobufTest.Message3.newBuilder().setName("test").setField1(0).setField2(1).build()).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream + System.currentTimeMillis();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<GeneratedMessageV3> deserializer = SerializerFactory.protobufMultiTypeDeserializer(serializerConfig, map);

                EventStreamReader<GeneratedMessageV3> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<GeneratedMessageV3> event = reader.readNextEvent(1000);
                assert null != event.getEvent();
                assert event.getEvent() instanceof ProtobufTest.Message1;
                event = reader.readNextEvent(1000);
                assert null != event.getEvent();
                assert event.getEvent() instanceof ProtobufTest.Message2;
                event = reader.readNextEvent(1000);
                assert null != event.getEvent();
                assert event.getEvent() instanceof ProtobufTest.Message3;

                // endregion
                // region read into writer schema
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, null);

                EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<DynamicMessage> genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = "rg3" + stream;
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map2 = new HashMap<>();
                // add only two schemas
                map2.put(ProtobufTest.Message1.class, schema1);
                map2.put(ProtobufTest.Message2.class, schema2);

                Serializer<Either<GeneratedMessageV3, DynamicMessage>> eitherDeserializer =
                        SerializerFactory.protobufTypedOrGenericDeserializer(serializerConfig, map2);

                EventStreamReader<Either<GeneratedMessageV3, DynamicMessage>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<GeneratedMessageV3, DynamicMessage>> e1 = reader3.readNextEvent(1000);
                assert e1.getEvent() != null;
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof ProtobufTest.Message1;
                
                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof ProtobufTest.Message2;
                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isRight();
                //endregion
            }
        }
    }
}


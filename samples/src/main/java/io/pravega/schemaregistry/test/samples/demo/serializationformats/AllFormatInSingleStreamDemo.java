/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.samples.demo.serializationformats;

import com.google.protobuf.DescriptorProtos;
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
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.samples.demo.objects.Address;
import io.pravega.schemaregistry.test.samples.demo.objects.DerivedUser1;
import io.pravega.schemaregistry.test.samples.generated.ProtobufTest;
import io.pravega.schemaregistry.test.samples.generated.Type1;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Scanner;

/**
 * This sample writes objects of all json protobuf and avro formats into a single stream. For this the `schema type` property
 * of the group is set as {@link SchemaType#Any}. 
 * During reads it uses {@link SerializerFactory#multiFormatGenericDeserializer(SerializerConfig)} to deserialize them into generic records 
 * of each type and the reader returns the common base class {@link Object}. 
 */
public class AllFormatInSingleStreamDemo {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;

    public AllFormatInSingleStreamDemo(ClientConfig clientConfig, SchemaRegistryClient client, String scope, String stream, String groupId) {
        this.clientConfig = clientConfig;
        this.client = client;
        this.scope = scope;
        this.stream = stream;
        initialize(groupId);
    }

    private void initialize(String groupId) {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Any;
        client.addGroup(groupId, schemaType,
                SchemaValidationRules.of(Compatibility.allowAny()),
                true, Collections.emptyMap());
    }

    public static void main(String[] args) {
        String scope = "scope" + System.currentTimeMillis();
        String stream = "stream";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.createRegistryClient(SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:9092")).build());
        AllFormatInSingleStreamDemo demo = new AllFormatInSingleStreamDemo(clientConfig, schemaRegistryClient, scope, stream, groupId);

        EventStreamWriter<Type1> avro = demo.createAvroWriter(groupId);
        EventStreamWriter<ProtobufTest.Message1> proto = demo.createProtobufWriter(groupId);
        EventStreamWriter<DerivedUser1> json = demo.createJsonWriter(groupId);
        EventStreamReader<Object> reader = demo.createReader(groupId);
        while (true) {
            System.out.println("choose: (1, 2 or 3)");
            System.out.println("1. write avro message");
            System.out.println("2. write protobuf message");
            System.out.println("3. write json message");
            System.out.println("4. read all messages");
            System.out.print("> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            try {
                int choice = Integer.parseInt(s);
                switch (choice) {
                    case 1:
                        avro.writeEvent(new Type1("a", 1)).join();
                        break;
                    case 2:
                        ProtobufTest.Message1 type1 = ProtobufTest.Message1.newBuilder().setName("test")
                                                                           .setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val3).build())
                                                                           .build();
                        proto.writeEvent(type1).join();
                        break;
                    case 3:
                        json.writeEvent(new DerivedUser1("json", new Address("a", "b"), 1, "users")).join();
                        break;
                    case 4:
                        EventRead<Object> event = reader.readNextEvent(1000);
                        while (event.getEvent() != null || event.isCheckpoint()) {
                            System.out.println("event read:" + event.getEvent());
                            event = reader.readNextEvent(1000);
                        }
                        break;
                    default:
                }
            } catch (NumberFormatException e) {
                System.err.println("invalid choice");
            }
        }
    }

    private EventStreamReader<Object> createReader(String groupId) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        Serializer<Object> deserializer = SerializerFactory.multiFormatGenericDeserializer(serializerConfig);
        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
    }

    private EventStreamWriter<Type1> createAvroWriter(String groupId) {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        AvroSchema<Type1> avro = AvroSchema.of(Type1.class);
        Serializer<Type1> serializer = SerializerFactory.avroSerializer(serializerConfig, avro);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private EventStreamWriter<DerivedUser1> createJsonWriter(String groupId) {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        JSONSchema<DerivedUser1> avro = JSONSchema.of(DerivedUser1.class);
        Serializer<DerivedUser1> serializer = SerializerFactory.jsonSerializer(serializerConfig, avro);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    @SneakyThrows
    private EventStreamWriter<ProtobufTest.Message1> createProtobufWriter(String groupId) {
        Path path = Paths.get("samples/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Serializer<ProtobufTest.Message1> serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.psearch;

import com.google.protobuf.DescriptorProtos;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.generated.Psearch;
import io.pravega.shared.NameUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TaxiTrafficGenerator {
    public static void main(String[] args) throws IOException {
        Random random = new Random();
        String scope = "dataPlaneScope";
        String stream = "testStream";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        Path path = Paths.get("samples/resources/proto/psearch.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<Psearch.Taxi> schema = ProtobufSchema.of(Psearch.Taxi.class, descriptorSet);
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create("http://localhost:9092"));
        SchemaRegistryClient client = RegistryClientFactory.createRegistryClient(config);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Serializer<Psearch.Taxi> serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<Psearch.Taxi> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        double lat = -73.99237823486328;
        double log = 40.66987228393555;
        for (int i = 0; i < 2; i++) {
            writer.writeEvent(Psearch.Taxi.newBuilder()
                                          .setTotalAmount(6.5)
                                          .setImprovementSurcharge(6.5)
                                          .setTollsAmount(6.5)
                                          .setPickupDatetime(new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").format(new Date()))
                                          .setDropoffDatetime(new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").format(new Date()))
                                          .setTripType("1")
                                          .setPassengerCount(random.nextInt(5))
                                          .setRateCodeId("1")
                                          .addPickupLocation(lat)
                                          .addPickupLocation(log)
                                          .addDropoffLocation(lat)
                                          .addDropoffLocation(log)
                                          .setFareAmount(6.5)
                                          .setExtra(6.5)
                                          .setTripDistance(6.5)
                                          .setTipAmount(6.5)
                                          .setStoreAndFwdFlag("proto")
                                          .setPaymentType("2")
                                          .setMtaTax(6.5)
                                          .setVendorId("1")
                                          .build()).join();
        }

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String readerGroupName = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        serializerConfig = SerializerConfig.builder().groupId(groupId)
                                           .registryConfigOrClient(Either.right(client)).build();
        Serializer<String> deserializer = SerializerFactory.deserializerAsJsonString(serializerConfig);
        EventStreamReader<String> reader = clientFactory.createReader("readerName", readerGroupName,
                deserializer,
                ReaderConfig.builder().build());

        for (int i = 0; i < 3; i++) {
            System.out.println(reader.readNextEvent(1000).getEvent());
        }
        System.exit(0);
    }
}

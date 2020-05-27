/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.samples.demo.messagebus.proto;

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
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.samples.generated.ProtobufTest;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SpecificAndGenericConsumerProto {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamReader<Either<GeneratedMessageV3, DynamicMessage>> reader;

    private SpecificAndGenericConsumerProto(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri((URI.create(registryUri))).build();
        client = SchemaRegistryClientFactory.createRegistryClient(config);
        this.scope = scope;
        this.stream = stream;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        initialize();
        this.reader = createReader(groupId);
    }

    public static void main(String[] args) {
        Options options = new Options();

        Option controllerUriOpt = new Option("c", "controllerUri", true, "Controller Uri");
        controllerUriOpt.setRequired(true);
        options.addOption(controllerUriOpt);

        Option registryUriOpt = new Option("r", "registryUri", true, "Registry Uri");
        registryUriOpt.setRequired(true);
        options.addOption(registryUriOpt);

        Option scopeOpt = new Option("sc", "scope", true, "scope");
        scopeOpt.setRequired(true);
        options.addOption(scopeOpt);

        Option streamOpt = new Option("st", "stream", true, "stream");
        streamOpt.setRequired(true);
        options.addOption(streamOpt);

        CommandLineParser parser = new BasicParser();
        
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Protobuf-consumer", options);
            
            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        SpecificAndGenericConsumerProto consumer = new SpecificAndGenericConsumerProto(controllerUri, registryUri, scope, stream);
        
        while (true) {
            EventRead<Either<GeneratedMessageV3, DynamicMessage>> event = consumer.consume();
            if (event.getEvent() != null) {
                Either<GeneratedMessageV3, DynamicMessage> record = event.getEvent();
                if (record.isLeft()) {
                    if (record.getLeft() instanceof ProtobufTest.Message1) {
                        ProtobufTest.Message1 type1 = (ProtobufTest.Message1) record.getLeft();
                        System.err.println("processing record of type ProtobufTest.Message1: " + type1);
                    } else if (record.getLeft() instanceof ProtobufTest.Message2) {
                        ProtobufTest.Message2 type2 = (ProtobufTest.Message2) record.getLeft();
                        System.err.println("processing record of type ProtobufTest.Message2: " + type2);
                    } else {
                        throw new IllegalArgumentException(" we should not be here ");
                    }
                } else {
                    DynamicMessage rec = record.getRight();
                    System.err.println("processing generic record " + rec);
                }
            }
        }
    }
    
    private void initialize() {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
    }

    @SneakyThrows
    private EventStreamReader<Either<GeneratedMessageV3, DynamicMessage>> createReader(String groupId) {
        Path path = Paths.get("samples/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.ofBaseType(ProtobufTest.Message1.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.ofBaseType(ProtobufTest.Message2.class, descriptorSet);

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoCreateGroup(SchemaType.Protobuf,
                                                                    SchemaValidationRules.of(Compatibility.allowAny()),
                                                                    true)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message1.class, schema1);
        map.put(ProtobufTest.Message2.class, schema2);
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<Either<GeneratedMessageV3, DynamicMessage>> deserializer = SerializerFactory.protobufTypedOrGenericDeserializer(serializerConfig, map);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        // endregion
    }

    private EventRead<Either<GeneratedMessageV3, DynamicMessage>> consume() {
        return reader.readNextEvent(1000);
    }
}

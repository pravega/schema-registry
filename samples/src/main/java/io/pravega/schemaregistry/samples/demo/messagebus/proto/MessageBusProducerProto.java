/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.messagebus.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.generated.ProtobufTest;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
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
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBusProducerProto {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamWriter<GeneratedMessageV3> writer;

    private MessageBusProducerProto(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri((URI.create(registryUri))).build();
        client = SchemaRegistryClientFactory.createRegistryClient(config);
        this.scope = scope;
        this.stream = stream;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        initialize();
        this.writer = createWriter(groupId);
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
            formatter.printHelp("Protobuf-producer", options);
            
            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        MessageBusProducerProto producer = new MessageBusProducerProto(controllerUri, registryUri, scope, stream);
        
        AtomicInteger counter = new AtomicInteger();

        while (true) {
            System.out.println("choose: (1, 2 or 3)");
            System.out.println("1. ProtobufTest.Message1");
            System.out.println("2. ProtobufTest.Message2");
            System.out.println("3. ProtobufTest.Message3");
            System.out.print("> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            try {
                int choice = Integer.parseInt(s);
                switch (choice) {
                    case 1:
                        ProtobufTest.InternalMessage build = ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val3).build();
                        ProtobufTest.Message1 type1 = ProtobufTest.Message1.newBuilder().setName("test" + counter.incrementAndGet())
                                                                           .setInternal(build).build();
                        producer.produce(type1).join();
                        System.out.println("Written event:\n" + type1);
                        break;
                    case 2:
                        ProtobufTest.Message2 type2 = ProtobufTest.Message2.newBuilder().setName("test" + counter.incrementAndGet()).setField1(counter.get()).build();
                        producer.produce(type2).join();
                        System.out.println("Written event:\n" + type2);
                        break;
                    case 3:
                        ProtobufTest.Message3 type3 = ProtobufTest.Message3.newBuilder().setName("test" + counter.incrementAndGet()).setField1(counter.get()).setField2(counter.get()).build();
                        producer.produce(type3).join();
                        System.out.println("Written event:\n" + type3);
                        break;
                    default:
                        System.err.println("invalid choice!");
                }
            } catch (NumberFormatException e) {
                System.err.println("invalid choice!");
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
    private EventStreamWriter<GeneratedMessageV3> createWriter(String groupId) {
        Path path = Paths.get("samples/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.ofBaseType(ProtobufTest.Message1.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.ofBaseType(ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema3 = ProtobufSchema.ofBaseType(ProtobufTest.Message3.class, descriptorSet);

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoCreateGroup(SerializationFormat.Protobuf,
                                                                    SchemaValidationRules.of(Compatibility.allowAny()),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message1.class, schema1);
        map.put(ProtobufTest.Message2.class, schema2);
        map.put(ProtobufTest.Message3.class, schema3);
        Serializer<GeneratedMessageV3> serializer = SerializerFactory.protobufMultiTypeSerializer(serializerConfig, map);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(GeneratedMessageV3 event) {
        return writer.writeEvent(event);
    }
}

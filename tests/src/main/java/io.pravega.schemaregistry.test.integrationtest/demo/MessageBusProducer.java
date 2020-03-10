/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo;

import com.google.common.collect.ImmutableList;
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
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerDeFactory;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.test.integrationtest.generated.Test1;
import io.pravega.schemaregistry.test.integrationtest.generated.Test2;
import io.pravega.schemaregistry.test.integrationtest.generated.Test3;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBusProducer {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamWriter<SpecificRecordBase> writer;

    private MessageBusProducer(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create(registryUri));
        client = RegistryClientFactory.createRegistryClient(config);
        this.scope = scope;
        this.stream = stream;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        initialize(groupId);
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
            formatter.printHelp("messagebus-producer", options);
            
            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        MessageBusProducer producer = new MessageBusProducer(controllerUri, registryUri, scope, stream);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        AtomicInteger counter = new AtomicInteger();

        while (true) {
            System.out.println("choose: (1, 2 or 3)");
            System.out.println("1. Test1");
            System.out.println("2. Test2");
            System.out.println("3. Test3");
            System.out.println("> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            try {
                int choice = Integer.parseInt(s);
                switch (choice) {
                    case 1:
                        Test1 test1 = new Test1("test1", counter.incrementAndGet());
                        producer.produce(test1).join();
                        System.out.println("Written event:" + test1);
                        break;
                    case 2:
                        Test2 test2 = new Test2("test2", counter.incrementAndGet(), "field2");
                        producer.produce(test2).join();
                        System.out.println("Written event:" + test2);
                        break;
                    case 3:
                        Test3 test3 = new Test3("test3", counter.incrementAndGet(), "field2", "field3");
                        producer.produce(test3).join();
                        System.out.println("Written event:" + test3);
                        break;
                    default:
                        System.err.println("invalid choice!");
                }
            } catch (Exception e) {
                System.err.println("invalid choice!");
            }
        }
    }
    
    private void initialize(String groupId) {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)),
                true, true);
    }

    private EventStreamWriter<SpecificRecordBase> createWriter(String groupId) {
        AvroSchema<SpecificRecordBase> schema1 = AvroSchema.of(Test1.class, Test1.getClassSchema());
        AvroSchema<SpecificRecordBase> schema2 = AvroSchema.of(Test2.class, Test2.getClassSchema());
        AvroSchema<SpecificRecordBase> schema3 = AvroSchema.of(Test3.class, Test3.getClassSchema());

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Test1.class, schema1);
        map.put(Test2.class, schema2);
        map.put(Test3.class, schema3);
        Serializer<SpecificRecordBase> serializer = SerDeFactory.multiplexedAvroSerializer(serializerConfig, map);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(SpecificRecordBase event) {
        return writer.writeEvent(event);
    }
}

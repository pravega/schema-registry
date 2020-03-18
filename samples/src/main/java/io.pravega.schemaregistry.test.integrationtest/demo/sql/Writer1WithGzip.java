/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.sql;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Writer1WithGzip {
    private static final Schema SCHEMA = SchemaBuilder
            .record("User")
            .fields()
            .name("name")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("age")
            .type(Schema.create(Schema.Type.INT))
            .noDefault()
            .endRecord();
    private static final Random RANDOM = new Random();
    
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamWriter<GenericRecord> writer;

    private Writer1WithGzip(String controllerURI, String registryUri, String scope, String stream) {
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
            formatter.printHelp("writer1", options);

            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");

        Writer1WithGzip producer = new Writer1WithGzip(controllerUri, registryUri, scope, stream);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        AtomicInteger integer = new AtomicInteger();

        Futures.loop(() -> true, () -> {
            Exceptions.handleInterrupted(() -> Thread.sleep(1000));
            return producer.produce("gzipwriter-" + integer.incrementAndGet());
        }, executor);
    }

    private void initialize(String groupId) {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(true)));
    }

    private EventStreamWriter<GenericRecord> createWriter(String groupId) {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .codec(CodecFactory.gzip())
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        AvroSchema<GenericRecord> schema = AvroSchema.of(SCHEMA);
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(String value) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("name", value);
        record.put("age", RANDOM.nextInt(100));

        return writer.writeEvent(record);
    }
}

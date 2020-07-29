/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.sql;

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
import io.pravega.schemaregistry.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.shared.serializers.SerializerConfig;
import io.pravega.shared.NameUtils;
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
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Writer2 {
    private static final Schema SCHEMA = SchemaBuilder
            .record("User")
            .fields()
            .name("name")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("age")
            .type(Schema.create(Schema.Type.INT))
            .noDefault()
            .name("address")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("homeless")
            .endRecord();
    private static final Random RANDOM = new Random();

    private final ClientConfig clientConfig;
    private final SchemaRegistryClientConfig config;
    private final String scope;
    private final String stream;
    private final EventStreamWriter<GenericRecord> writer;

    private Writer2(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        this.config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(registryUri)).build();
        this.scope = scope;
        this.stream = stream;
        String groupId = NameUtils.getScopedStreamName(scope, stream);
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
            formatter.printHelp("writer2", options);

            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");

        Writer2 producer = new Writer2(controllerUri, registryUri, scope, stream);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        AtomicInteger integer = new AtomicInteger();
        Futures.loop(() -> true, () -> {
            Exceptions.handleInterrupted(() -> Thread.sleep(1000));
            return producer.produce("writer2-" + integer.incrementAndGet(), "address-" + integer.get());
        }, executor);
    }

    private void initialize() {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
    }

    private EventStreamWriter<GenericRecord> createWriter(String groupId) {

        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(SerializationFormat.Avro, Compatibility.backward(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryConfig(config)
                                                            .build();

        AvroSchema<GenericRecord> schema = AvroSchema.ofRecord(SCHEMA);
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
        // endregion

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    private CompletableFuture<Void> produce(String aValue, String bValue) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("name", aValue);
        record.put("age", RANDOM.nextInt(100));
        record.put("address", bValue);

        return writer.writeEvent(record)
                .thenAccept(v -> System.out.println(record));
    }
}

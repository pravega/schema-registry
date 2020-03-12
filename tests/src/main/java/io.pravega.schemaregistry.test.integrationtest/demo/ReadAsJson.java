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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.serializers.SerDeFactory;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
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

public class ReadAsJson {
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final EventStreamReader<GenericRecord> reader;

    private ReadAsJson(String controllerURI, String registryUri, String scope, String stream) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create(registryUri));
        client = RegistryClientFactory.createRegistryClient(config);
        this.scope = scope;
        this.stream = stream;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        initialize(groupId);
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
            formatter.printHelp("to-json-app", options);
            
            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");
        String scope = cmd.getOptionValue("scope");
        String stream = cmd.getOptionValue("stream");
        
        ReadAsJson consumer = new ReadAsJson(controllerUri, registryUri, scope, stream);
        
        while (true) {
            EventRead<GenericRecord> event = consumer.consume();
            if (event.getEvent() != null) {
                GenericRecord record = event.getEvent();
                String json = record.toString();
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                JsonParser jp = new JsonParser();
                JsonElement je = jp.parse(json);
                String prettyJsonString = gson.toJson(je);
                System.err.println(prettyJsonString);
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
                SchemaValidationRules.of(Compatibility.backward()),
                true, Collections.singletonMap(SerDeFactory.ENCODE, Boolean.toString(true)));
    }

    private EventStreamReader<GenericRecord> createReader(String groupId) {
        // region serializer
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        // endregion
        
        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg-generic" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, null);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        // endregion
    }

    private EventRead<GenericRecord> consume() {
        return reader.readNextEvent(1000);
    }
}

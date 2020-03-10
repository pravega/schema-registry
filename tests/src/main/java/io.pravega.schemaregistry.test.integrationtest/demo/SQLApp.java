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

import com.google.common.base.Charsets;
import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Scanner;

public class SQLApp {
    private  final SchemaRegistryClient client;
    public SQLApp(SchemaRegistryClient client) {
        this.client = client;
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();

        Option controllerUriOpt = new Option("c", "controllerUri", true, "Controller Uri");
        controllerUriOpt.setRequired(true);
        options.addOption(controllerUriOpt);

        Option registryUriOpt = new Option("r", "registryUri", true, "Registry Uri");
        registryUriOpt.setRequired(true);
        options.addOption(registryUriOpt);
        
        CommandLineParser parser = new BasicParser();

        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("sql", options);

            System.exit(-1);
        }

        String controllerUri = cmd.getOptionValue("controllerUri");
        String registryUri = cmd.getOptionValue("registryUri");

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerUri)).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create(registryUri));
        SchemaRegistryClient client = RegistryClientFactory.createRegistryClient(config);

        while (true) {
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            String[] tokens = s.split(" ");
            if (tokens[0].toLowerCase().equals("create") && tokens[1].toLowerCase().equals("table")) {
                SQLApp sqlApp = new SQLApp(client);
                sqlApp.handleCreateTable(tokens);
            } else if (tokens[0].toLowerCase().equals("select")) {
                SQLApp sqlApp = new SQLApp(client);
                sqlApp.handleSelect(tokens);
            }
        }
    }

    private void handleCreateTable(String[] tokens) {
        String tableName = tokens[3];
        String from = tokens[4];
        assert from.toLowerCase().equals("from");

        String qualifiedStreamName = tokens[5];
        String[] streamToken = qualifiedStreamName.split("/");
        String scope = streamToken[0];
        String stream = streamToken[1];
        createTable(scope, stream);
    }

    private void createTable(String scope, String stream) {
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        GroupProperties properties = client.getGroupProperties(groupId);
        
        SchemaWithVersion latestSchema = client.getLatestSchema(groupId, null);
        SchemaInfo schemaInfo = latestSchema.getSchema();

        switch (properties.getSchemaType()) {
            case Avro:
                Schema schema = new Schema.Parser().parse(new String(schemaInfo.getSchemaData(), Charsets.UTF_8));
                schema.getFields().stream().map(x -> {
                    String name = x.name();
                    Schema.Type type = x.schema().getType();
                    switch (type) {
                        case STRING:
                        case ENUM:
                        case INT:
                        case LONG:
                    }
                    return null;
                });
                break;

        }
    }
    
    List<GenericRecord> handleSelect(String[] tokens) {
        // read all events in the table and keep printing them.
        return null;
    }
}

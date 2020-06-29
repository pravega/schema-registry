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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLApp {
    private  final SchemaRegistryClient client;
    private  final ClientConfig clientConfig;
    public SQLApp(ClientConfig clientConfig, SchemaRegistryClient client) {
        this.clientConfig = clientConfig;
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
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create(registryUri)).build();
        SchemaRegistryClient client = SchemaRegistryClientFactory.createRegistryClient(config);

        while (true) {
            System.out.println("sql> ");
            Scanner in = new Scanner(System.in);
            String s = in.nextLine();
            String[] tokens = s.split(" ");
            if (tokens[0].toLowerCase().equals("create") && tokens[1].toLowerCase().equals("table")) {
                SQLApp sqlApp = new SQLApp(clientConfig, client);
                sqlApp.handleCreateTable(tokens);
            } else if (tokens[0].toLowerCase().equals("select") && tokens[1].equals("*")) {
                SQLApp sqlApp = new SQLApp(clientConfig, client);
                sqlApp.handleSelect(tokens);
            } else if (tokens[0].toLowerCase().equals("show") && tokens[1].equals("tables")) {
                SQLApp sqlApp = new SQLApp(clientConfig, client);
                sqlApp.handleShowTables();
            }
        }
    }

    private void handleShowTables() {
        val groups = Lists.newArrayList(client.listGroups());
        groups.stream().filter(x -> x.getKey().startsWith("table://"))
              .forEach(x -> {
                  String tableName = x.getKey().substring("table://".length());
                  String tableGroupId = getTableGroupId(tableName);
                  SchemaWithVersion tableSchema = client.getLatestSchemaVersion(tableGroupId, null);

                  // read into this table schema
                  TableSchema schema = TableSchema.fromBytes(tableSchema.getSchemaInfo().getSchemaData().array());

                  System.out.println("Table name: " + tableName);

                  schema.columns.forEach(column -> {
                      System.out.print(column.name);
                      System.out.print(":");
                      System.out.print(column.getColumnType().name());
                      System.out.println();
                  });
              });
    }

    private void handleCreateTable(String[] tokens) throws UnsupportedEncodingException {
        String tableName = tokens[2];
        String from = tokens[3];
        assert from.toLowerCase().equals("from");

        String qualifiedStreamName = tokens[4];
        String[] streamToken = qualifiedStreamName.split("[/]");
        String scope = streamToken[1];
        String stream = streamToken[2];
        createTable(scope, stream, tableName);
    }

    private void createTable(String scope, String stream, String tableName) {
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        GroupProperties properties = client.getGroupProperties(groupId);
        
        SchemaWithVersion latestSchema = client.getLatestSchemaVersion(groupId, null);
        SchemaInfo schemaInfo = latestSchema.getSchemaInfo();
        TableSchema tableSchema = null;
        switch (properties.getSerializationFormat()) {
            case Avro:
                Schema schema = new Schema.Parser().parse(new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8));
                tableSchema = new TableSchema(schema.getFields().stream().map(x -> {
                    String name = x.name();
                    Schema.Type type = x.schema().getType();
                    return new Column(name, searchEnum(ColumnType.class, type.getName()));
                }).collect(Collectors.toList()));
                break;
            default:
                throw new NotImplementedException("");
        }

        String tableGroupId = getTableGroupId(tableName);
        Compatibility compatibility = Compatibility.denyAll();
        SerializationFormat serializationFormat = SerializationFormat.custom("table");

        ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
        map.put("scope", scope);
        map.put("stream", stream);
        
        SchemaInfo tableSchemaInfo = new SchemaInfo("table", serializationFormat, ByteBuffer.wrap(tableSchema.toBytes()), map.build());
        
        client.addGroup(tableGroupId, new GroupProperties(serializationFormat, compatibility, false));
        client.addSchema(tableGroupId, tableSchemaInfo);
    }

    @SneakyThrows
    private String getTableGroupId(String tableName) {
        return "table://" + tableName;
    }

    List<GenericRecord> handleSelect(String[] tokens) {
        String from = tokens[2];
        assert from.toLowerCase().equals("from");

        String tableName = tokens[3];
        
        String tableGroupId = getTableGroupId(tableName);
        SchemaWithVersion tableSchema = client.getLatestSchemaVersion(tableGroupId, null);
        Map<String, String> prop = tableSchema.getSchemaInfo().getProperties();
        String scope = prop.get("scope");
        String stream = prop.get("stream");
        
        // read into this table schema
        TableSchema schema = TableSchema.fromBytes(tableSchema.getSchemaInfo().getSchemaData().array());
        StringBuilder top = new StringBuilder();
        for (int i = 0; i < schema.columns.size(); i++) {
            top.append("_");
        }
        System.out.println(top.toString());
        
        System.out.print("|");

        schema.columns.forEach(column -> {
            String name = leftPad(rightPad(column.name));
            System.out.print(name);
            System.out.print("|");
        });

        Predicate<GenericRecord> predicate = x -> true;
        if (tokens.length > 4) {
            assert tokens[4].equals("where");

            Column c = schema.columns.stream().filter(x -> x.name.equals(tokens[5])).findAny().get();
            predicate = getPredicate(c, tokens[6], tokens[7]);

        }

        return printColumns(scope, stream, schema, predicate);
    }

    private Predicate<GenericRecord> getPredicate(Column column, String operator, String value) {
        Operator op;
        switch (operator) {
            case ">":
                op = new GreaterThan(column, value);
                break;
            case "<":
                op = new LessThan(column, value);
                break;
            case "=":
                op = new EqualTo(column, value);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return op.getPredicate();
    }

    private String rightPad(String column) {
        int padding = 15 - column.length();

        StringBuilder builder = new StringBuilder();
        builder.append(column);
        for (int i = 0; i < padding; i++) {
            builder.append(" ");
        }
        return builder.toString();
    }
    
    private String leftPad(String column) {
        int padding = 30 - column.length();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < padding; i++) {
            builder.append(" ");
        }
        builder.append(column);

        return builder.toString();
    }

    private List<GenericRecord> printColumns(String scope, String stream, TableSchema tableSchema, Predicate<GenericRecord> predicate) {
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        String rg = "rg" + scope + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        SchemaWithVersion latestSchema = client.getLatestSchemaVersion(groupId, null);
        AvroSchema<GenericRecord> avroSchema = AvroSchema.ofRecord(new Schema.Parser().parse(new String(latestSchema.getSchemaInfo().getSchemaData().array(), Charsets.UTF_8)));
        Serializer<GenericRecord> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, avroSchema);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        while (true) {
            EventRead<GenericRecord> event = reader.readNextEvent(1000);
            GenericRecord record = event.getEvent();
            if (record == null || !predicate.test(record)) {
                continue;
            }
            
            System.out.print("|");
            
            tableSchema.columns.forEach(column -> {
                Object val = record.get(column.name);
                switch (column.columnType) {
                    case STRING:
                        System.out.print(leftPad(rightPad(val.toString())));
                        break;
                    case INT:
                        System.out.print(leftPad(rightPad(Integer.toString((Integer) val))));
                        break;
                    case LONG:
                        System.out.print(leftPad(rightPad(Long.toString((Long) val))));
                        break;
                }
                System.out.print("|");
            });
            System.out.println("");
        }
    }

    enum ColumnType {
        INT,
        STRING,
        LONG 
    }
    
    @Data
    static class TableSchema {
        private final List<Column> columns;
        
        @SneakyThrows
        public byte[] toBytes() {
            StringBuilder builder = new StringBuilder();
            columns.forEach(column -> {
                builder.append(column.name);
                builder.append(",");
                builder.append(column.columnType.name());
                builder.append(",");
            });
            return builder.toString().getBytes(Charsets.UTF_8.name());
        }
        
        static TableSchema fromBytes(byte[] schemaData) {
            String schema = new String(schemaData, Charsets.UTF_8);
            String[] tokens = schema.split(",");
            List<Column> columns = new LinkedList<>();
            
            for (int i = 0; i < tokens.length; i = i + 2) {
                String name = tokens[i];
                String type = tokens[i + 1];
                columns.add(new Column(name, searchEnum(ColumnType.class, type)));
            }
            return new TableSchema(columns);
        }
    }
    
    @Data
    static class Column {
        private final String name;
        private final ColumnType columnType;
    }

    private static <T extends Enum<?>> T searchEnum(Class<T> enumeration,
                                                    String search) {
        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        throw new IllegalArgumentException();
    }

    private interface Operator {
        Predicate<GenericRecord> getPredicate();
    }
    
    @Data
    private static class GreaterThan implements Operator {
        private final Column column;
        private final String value;

        @Override
        public Predicate<GenericRecord> getPredicate() {
            switch (column.getColumnType()) {
                case INT:
                    int intVal = Integer.parseInt(value);
                    return x -> (int) x.get(column.name) > intVal;
                case LONG:
                    long longVal = Long.parseLong(value);
                    return x -> (long) x.get(column.name) > longVal;
                case STRING:
                    default:
                    throw new IllegalArgumentException();
            }
        }
    }

    @Data
    private static class LessThan implements Operator {
        private final Column column;
        private final String value;

        @Override
        public Predicate<GenericRecord> getPredicate() {
            switch (column.getColumnType()) {
                case INT:
                    int intVal = Integer.parseInt(value);
                    return x -> (int) x.get(column.name) < intVal;
                case LONG:
                    long longVal = Long.parseLong(value);
                    return x -> (long) x.get(column.name) < longVal;
                case STRING:
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    @Data
    private static class EqualTo implements Operator {
        private final Column column;
        private final String value;

        @Override
        public Predicate<GenericRecord> getPredicate() {
            switch (column.getColumnType()) {
                case INT:
                    int intVal = Integer.parseInt(value);
                    return x -> (int) x.get(column.name) == intVal;
                case LONG:
                    long longVal = Long.parseLong(value);
                    return x -> (long) x.get(column.name) == longVal;
                case STRING:
                    return x -> value.equals(((Utf8) x.get(column.name)).toString());
                default:
                    throw new IllegalArgumentException();
            }
        }
    }
}

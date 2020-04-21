/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.samples.demo.encoding;

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
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * A sample class to demonstrate different kind of compression encodings that can be used with Pravega serializers.
 * It has samples for gzip, snappy, none and custom encodings. 
 * This highlights the power of encoding headers with pravega payload where schema registry facilitates clients recording
 * different encoding schemes used for encoding the data into the header used before the data. 
 */
@Slf4j
public class CompressionDemo {
    private static final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    private static final String MYCOMPRESSION = "mycompression";
    private static final Codec MY_CODEC = new Codec() {
        @Override
        public CodecType getCodecType() {
            return CodecType.custom(MYCOMPRESSION, Collections.emptyMap());
        }

        @Override
        public ByteBuffer encode(ByteBuffer data) {
            // left rotate by 1 byte
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i; 
            byte temp = array[0];
            for (i = 0; i < array.length - 1; i++) {
                array[i] = array[i + 1];
            }
            array[array.length - 1] = temp;
            return ByteBuffer.wrap(array);
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            int i;
            byte temp = array[array.length - 1];
            for (i = array.length - 1; i > 0; i--) {
                array[i] = array[i - 1];
            }
            array[0] = temp;
            return ByteBuffer.wrap(array);
        }
    };
    private static final Random RANDOM = new Random();

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;
    private final String scope;
    private final String stream;
    private final String groupId;
    private final AvroSchema<GenericRecord> schema1;
    private final EventStreamClientFactory clientFactory;
    private final EventStreamReader<GenericRecord> reader;
    
    public CompressionDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create("http://localhost:9092"));
        client = RegistryClientFactory.createRegistryClient(config);
        id = Long.toString(System.currentTimeMillis());
        scope = "scope" + id;
        stream = "avrocompression";
        groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        schema1 = AvroSchema.of(SCHEMA1);
        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        initialize();
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .addDecoder(MY_CODEC.getCodecType(), MY_CODEC::decode)
                                                             .registryConfigOrClient(Either.right(client))
                                                             .build();

        Serializer<GenericRecord> readerDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String readerGroup = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(readerGroup,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        reader = clientFactory.createReader("r2", readerGroup, readerDeserializer, ReaderConfig.builder().build());
    }
    
    public static void main(String[] args) {
        CompressionDemo demo = new CompressionDemo();
        while (true) {
            System.out.println("1. Gzip");
            System.out.println("2. Snappy");
            System.out.println("3. None");
            System.out.println("4. Custom");
            System.out.println("5. Print all codecs");
            System.out.println("6. Read all");
            System.out.println("7. exit");
            Scanner in = new Scanner(System.in);
            int s;
            try {
                s = Integer.parseInt(in.nextLine());

                int size;
                switch (s) {
                    case 1:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());

                        demo.writeGzip(generateBigString(size));
                        break;
                    case 2:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.writeSnappy(generateBigString(size));
                        break;
                    case 3:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.withoutCompression(generateBigString(size));
                        break;
                    case 4:
                        System.out.println("enter size in kb");
                        in = new Scanner(System.in);
                        size = Integer.parseInt(in.nextLine());
                        demo.writeCustom(generateBigString(size));
                        break;
                    case 5:
                        demo.printAllCodecs();
                        break;
                    case 6:
                        demo.readMessages();
                        break;
                    case 7:
                        System.exit(0);
                        break;
                    default:
                        System.err.println("invalid choice");
                        break;
                }
            } catch (NumberFormatException e) {
                System.out.println("invalid choice");
                continue;
            }
        }
    }
    
    private void initialize() {
        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SchemaType schemaType = SchemaType.Avro;
            client.addGroup(groupId, schemaType, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        }
    }

    private void printAllCodecs() {
        List<CodecType> list = client.getCodecs(groupId);
        System.out.println(list);
    }
    
    private void writeGzip(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .autoRegisterCodec(true)
                                                            .codec(CodecFactory.gzip())
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }
    
    private void writeSnappy(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .codec(CodecFactory.snappy())
                                                            .autoRegisterSchema(true)
                                                            .autoRegisterCodec(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }
    
    private void writeCustom(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .codec(MY_CODEC)
                                                            .autoRegisterSchema(true)
                                                            .autoRegisterCodec(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());

        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();
    }

    private void withoutCompression(String input) {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .autoRegisterCodec(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        
        // region writer with schema1
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", input).build();

        writer.writeEvent(record).join();

    }

    private static String generateBigString(int sizeInKb) {
        byte[] array = new byte[1024 * sizeInKb];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }

    private void readMessages() {
        EventRead<GenericRecord> event = reader.readNextEvent(1000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            GenericRecord e = event.getEvent();
            System.out.println("event read = " + e.toString());
            event = reader.readNextEvent(1000);
        }
    }
}


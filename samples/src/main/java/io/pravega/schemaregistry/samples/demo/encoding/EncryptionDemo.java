/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.encoding;

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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.generated.Test1;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;

/**
 * A sample class to demonstrate how a custom encoding can be used. It uses AES algorithm to encrypt and decrypt each event
 * after it is serialized.  
 */
@Slf4j
public class EncryptionDemo {
    private static final String ALGORITHM = "AES";

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;
    private final String scope;
    private final String stream;
    private final String groupId;
    private final String myEncryption;
    private final EventStreamClientFactory clientFactory;
    private final String encryptionkey;

    private EncryptionDemo(String encryptionkey) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:9092")).build();
        client = SchemaRegistryClientFactory.createRegistryClient(config);
        id = Long.toString(System.currentTimeMillis());
        scope = "scope" + id;
        stream = "avroEncryption";
        groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        myEncryption = "myEncryption";
        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        this.encryptionkey = encryptionkey;
        initialize();
    }

    public static void main(String[] args) {
        EncryptionDemo demo = new EncryptionDemo("myEncryptionKeys");

        demo.startWriter();

        demo.startReader();

        System.exit(0);
    }

    private void initialize() {
        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }
    }

    private void startWriter() {
        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);

        // region writer with custom codec
        Codec myCodec = getMyCodec(encryptionkey);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(SerializationFormat.Avro,
                                                                    Compatibility.backward(), true)
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .codec(myCodec)
                                                            .registryClient(client)
                                                            .build();

        Serializer<Test1> serializer5 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer2 = clientFactory.createEventWriter(stream, serializer5, EventWriterConfig.builder().build());
        System.out.println("supply string");
        Scanner in = new Scanner(System.in);
        String input = in.nextLine();

        writer2.writeEvent(new Test1(input, 1)).join();
    }

    private void startReader() {
        List<String> list = client.getCodecTypes(groupId);
        assert 1 == list.size();
        assert list.stream().anyMatch(x -> x.equals(myEncryption));
        Codec myCodec = getMyCodec(encryptionkey);

        // region new reader with additional codec
        // add new decoder for custom serialization
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .addDecoder(myCodec.getCodecType(), myCodec::decode)
                                                             .registryClient(client)
                                                             .build();

        Serializer<GenericRecord> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
            String rg = "rg" + stream + System.currentTimeMillis();
            readerGroupManager.createReaderGroup(rg,
                    ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

            EventStreamReader<GenericRecord> reader = clientFactory.createReader("r2", rg, deserializer, ReaderConfig.builder().build());

            EventRead<GenericRecord> event = reader.readNextEvent(1000);
            while (event.isCheckpoint() || event.getEvent() != null) {
                GenericRecord e = event.getEvent();
                System.out.println(e.toString());
                event = reader.readNextEvent(1000);
            }
        }
        // endregion
    }

    @SneakyThrows
    private Codec getMyCodec(String encryptionkey) {
        return new Codec() {
            private final byte[] key = encryptionkey.getBytes(Charsets.UTF_8);

            @Override
            public String getCodecType() {
                return myEncryption;
            }

            @SneakyThrows
            @Override
            public ByteBuffer encode(ByteBuffer data) {
                SecretKeySpec secretKey = new SecretKeySpec(key, ALGORITHM);
                Cipher cipher = Cipher.getInstance(ALGORITHM);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey);

                byte[] array = new byte[data.remaining()];
                data.get(array);

                byte[] encrypted = cipher.doFinal(array);

                System.out.println("encoded as = " + new String(encrypted, Charsets.UTF_8));
                return ByteBuffer.wrap(encrypted);
            }

            @SneakyThrows
            @Override
            public ByteBuffer decode(ByteBuffer data) {
                byte[] array = new byte[data.remaining()];
                data.get(array);

                SecretKeySpec secretKey = new SecretKeySpec(key, ALGORITHM);
                Cipher cipher = Cipher.getInstance(ALGORITHM);
                cipher.init(Cipher.DECRYPT_MODE, secretKey);

                return ByteBuffer.wrap(cipher.doFinal(array));
            }
        };
    }
}


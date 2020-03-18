/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.encoding;

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
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.test.integrationtest.generated.Test1;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

@Slf4j
public class EncryptionDemo {
    private static final String ALGORITHM = "AES";
    private static final String ENCRYPTION_KEY_KEY = "key";

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;
    private final String scope;
    private final String stream;
    private final String groupId;
    private final String myEncryption;
    private final EventStreamClientFactory clientFactory;
    
    private EncryptionDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create("http://localhost:9092"));
        client = RegistryClientFactory.createRegistryClient(config);
        id = Long.toString(System.currentTimeMillis());
        scope = "scope" + id;
        stream = "avroEncryption";
        groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        myEncryption = "myEncryption";
        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        initialize();
    }
    
    public static void main(String[] args) {
        EncryptionDemo demo = new EncryptionDemo();
        String myEncryptionKey = "myEncryptionKeys";

        demo.startWriter(myEncryptionKey);
        
        demo.startReader();
        
        System.exit(0);
    }

    private void initialize() {
        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SchemaType schemaType = SchemaType.Avro;
            client.addGroup(groupId, schemaType,
                    SchemaValidationRules.of(Compatibility.backward()),
                    true, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(true)));
        }
    }
    
    private void startWriter(String myEncryptionKey) {
        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);
        
        // region writer with custom codec
        Map<String, String> properties = Collections.singletonMap(ENCRYPTION_KEY_KEY, myEncryptionKey);
        Codec myCodec = getMyCodec(properties);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .codec(myCodec)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        Serializer<Test1> serializer5 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer2 = clientFactory.createEventWriter(stream, serializer5, EventWriterConfig.builder().build());
        System.out.println("supply string");
        Scanner in = new Scanner(System.in);
        String input = in.nextLine();

        writer2.writeEvent(new Test1(input, 1)).join();
    }
    
    private void startReader() {
        List<CodecType> list = client.getCodecs(groupId);
        assert 1 == list.size();
        assert list.stream().anyMatch(x -> x.equals(CodecType.Custom) && x.getCustomTypeName().equals(myEncryption));
        Map<String, String> propertiesFromRegistry = list.get(0).getProperties();
        Codec myCodec = getMyCodec(propertiesFromRegistry);

        // region new reader with additional codec
        // add new decode for custom serialization
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .addDecoder(myCodec.getCodecType(), myCodec::decode)
                                                             .registryConfigOrClient(Either.right(client))
                                                             .build();

        Serializer<GenericRecord> deserializer = SerializerFactory.genericAvroDeserializer(serializerConfig2, null);

        try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
            String rg = "rg" + stream + System.currentTimeMillis();
            readerGroupManager.createReaderGroup(rg,
                    ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

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
    private Codec getMyCodec(Map<String, String> properties) {
        return new Codec() {
            private final byte[] key = properties.get("key").getBytes(Charsets.UTF_8);

            @Override
            public CodecType getCodecType() {
                return CodecType.custom(myEncryption, properties);
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

    private static class TestClass {
        private final String test;

        private TestClass(String test) {
            this.test = test;
        }

        public String getTest() {
            return test;
        }
    }
}


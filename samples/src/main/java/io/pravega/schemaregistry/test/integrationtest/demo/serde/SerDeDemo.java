/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest.demo.serde;

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
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.SneakyThrows;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SerDeDemo {
    private static final String DESERIALIZER_CLASS_NAME = "DeserializerClassName";
    private static final String SERIALIZER_CLASS_NAME = "SerializerClassName";
    private final ClientConfig clientConfig;
    private final SchemaRegistryClient client;
    private final String scope;
    private final String stream;
    private final String groupId;
    private final String filePath;

    private SerDeDemo(String controllerURI, String registryUri, String scope, String stream, String filePath) {
        clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI)).build();
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(URI.create(registryUri));
        client = RegistryClientFactory.createRegistryClient(config);
        this.scope = scope;
        this.stream = stream;
        this.groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);
        this.filePath = filePath;
        createScopeAndStream(scope, stream, groupId);
    }

    public static void main(String[] args) {
        String filePath = args[0];
        SerDeDemo serDeDemo = new SerDeDemo("tcp://localhost:9090", "http://localhost:9092", 
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), filePath);
        EventStreamWriter<MyPojo> writer = serDeDemo.createWriter();
        EventStreamReader<Object> reader = serDeDemo.createReader();
        writer.writeEvent(new MyPojo("test"));
        EventRead<Object> event = reader.readNextEvent(1000);
        assert event.getEvent() instanceof MyPojo;
        System.out.println(event.getEvent().toString());
        System.exit(0);
    }
    
    private void createScopeAndStream(String scope, String stream, String groupId) {
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.custom("serDe");
        client.addGroup(groupId, schemaType,
                SchemaValidationRules.of(Compatibility.denyAll()),
                false, Collections.singletonMap(SerializerFactory.ENCODE, Boolean.toString(false)));
    }

    @SneakyThrows
    private EventStreamWriter<MyPojo> createWriter() {
        SerializerConfig config = SerializerConfig.builder()
                                                  .groupId(groupId)
                                                  .autoRegisterSchema(true)
                                                  .registryConfigOrClient(Either.right(client))
                                                  .build();

        Map<String, String> map = new HashMap<>();
        map.put(SERIALIZER_CLASS_NAME, MySerializer.class.getName());
        map.put(DESERIALIZER_CLASS_NAME, MyDeserializer.class.getName());
        SchemaType schemaType = SchemaType.custom("serDe");

        Path path = Paths.get(filePath);

        URL url = path.toUri().toURL();

        SchemaInfo schemaInfo = new SchemaInfo("serde", schemaType, url.toString().getBytes(Charsets.UTF_8), map);
        MySerializer mySerializer = new MySerializer();

        Serializer<MyPojo> serializer = SerializerFactory.customSerializer(config, () -> schemaInfo, mySerializer);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        return clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private EventStreamReader<Object> createReader() {
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        SchemaInfo schema = client.getLatestSchema(groupId, null).getSchema();
        String urlString = new String(schema.getSchemaData(), Charsets.UTF_8);
        URL url = new URL(urlString);

        PravegaDeserializer<Object> myDeserializer = SerdeLoader.getDeserializer(schema.getProperties().get(DESERIALIZER_CLASS_NAME), url);
        Serializer<Object> deserializer = SerializerFactory.customDeserializer(serializerConfig, () -> schema, myDeserializer);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        return clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        // endregion
    }
}

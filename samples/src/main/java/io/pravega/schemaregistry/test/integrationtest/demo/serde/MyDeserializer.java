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

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializers.PravegaDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class MyDeserializer implements PravegaDeserializer<MyPojo> {
    @Override
    public MyPojo deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        ObjectInputStream oin;
        try {
            oin = new ObjectInputStream(inputStream);
            return (MyPojo) oin.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

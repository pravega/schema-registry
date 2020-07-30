/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.serde;

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.shared.impl.CustomDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class MyDeserializer implements CustomDeserializer<MyPojo> {
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

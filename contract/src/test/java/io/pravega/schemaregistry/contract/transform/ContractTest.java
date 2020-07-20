/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Random;

public class ContractTest {
    private static final Random RANDOM = new Random();

    @Test
    public void testObjectCreation() {
        ByteBuffer wrap = ByteBuffer.wrap(new byte[0]);
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i <= 100; i++) {
            builder.put(Integer.toString(i), "a");
        }
        ImmutableMap<String, String> mapOf101Entries = builder.build();
        builder = new ImmutableMap.Builder<>();
        String bigString = getBigString(201);

        builder.put(bigString, bigString);
        ImmutableMap<String, String> mapOfBigSizedEntries = builder.build();
        
        // schemainfo
        AssertExtensions.assertThrows("Null type not allowed", 
                () -> new SchemaInfo(null, SerializationFormat.Avro, wrap, ImmutableMap.of()),
            e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Null format not allowed",
                () -> new SchemaInfo("", null, wrap, ImmutableMap.of()),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("SerializationFormat `Any` not allowed",
                () -> new SchemaInfo("", SerializationFormat.Any, wrap, ImmutableMap.of()),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Null data not allowed",
                () -> new SchemaInfo("", SerializationFormat.Avro, null, ImmutableMap.of()),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Null properties not allowed",
                () -> new SchemaInfo("", SerializationFormat.Avro, wrap, null),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Schema binary max size - 8mb",
                () -> new SchemaInfo("", SerializationFormat.Avro, ByteBuffer.wrap(new byte[9 * 1024 * 1024]), ImmutableMap.of()),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Schema properties less than 200 bytes each",
                () -> new SchemaInfo("", SerializationFormat.Avro, wrap, mapOfBigSizedEntries),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Max properties allowed is 100",
                () -> new SchemaInfo("", SerializationFormat.Avro, wrap, mapOf101Entries),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        
        // group properties
        AssertExtensions.assertThrows("Null format not allowed",
                () -> new GroupProperties(null, Compatibility.backward(), false),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Null compatibility not allowed",
                () -> new GroupProperties(SerializationFormat.Any, null, false),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Null properties not allowed",
                () -> new GroupProperties(SerializationFormat.Any, Compatibility.backward(), false, null),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Group properties less than 200 bytes each",
                () -> new GroupProperties(SerializationFormat.Avro, Compatibility.backward(), false, mapOfBigSizedEntries),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Max properties allowed is 100",
                () -> new GroupProperties(SerializationFormat.Avro, Compatibility.backward(), false, mapOf101Entries),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        
        // codec type
        AssertExtensions.assertThrows("Null name not allowed",
                () -> new CodecType(null, ImmutableMap.of()),
                e -> Exceptions.unwrap(e) instanceof NullPointerException);
        AssertExtensions.assertThrows("Codec properties less than 900 kb overall",
                () -> new CodecType("", ImmutableMap.of(getBigString(1024 * 1024), "a")),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Codec properties less than 900 kb overall",
                () -> new CodecType("", ImmutableMap.of("a", getBigString(1024 * 1024))),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        
    }

    private String getBigString(int size) {
        byte[] array = new byte[size];
        RANDOM.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }
}

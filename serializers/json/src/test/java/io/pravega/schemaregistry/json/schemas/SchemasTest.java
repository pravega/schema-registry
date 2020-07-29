/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.json.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.shared.testobjs.DerivedUser1;
import io.pravega.schemaregistry.shared.testobjs.DerivedUser2;
import io.pravega.schemaregistry.shared.testobjs.User;
import org.junit.Test;

import static io.pravega.schemaregistry.json.testobjs.SchemaDefinitions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SchemasTest {
    @Test
    public void testJsonSchema() {
        JSONSchema<User> schema = JSONSchema.of(User.class);
        assertNotNull(schema.getSchema());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);

        JSONSchema<String> schema2 = JSONSchema.of("Person", JSON_SCHEMA_STRING, String.class);
        assertNotNull(schema2.getSchema());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
        
        JSONSchema<JsonNode> schema3 = JSONSchema.of("", JSON_SCHEMA_STRING_DRAFT_4, JsonNode.class);
        assertNotNull(schema3.getSchema());
        assertEquals(schema3.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);

        JSONSchema<JsonNode> schema4 = JSONSchema.of("", JSON_SCHEMA_STRING_DRAFT_7, JsonNode.class);
        assertNotNull(schema4.getSchema());
        assertEquals(schema4.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);

        JSONSchema<User> baseSchema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
        assertNotNull(baseSchema1.getSchema());
        assertEquals(baseSchema1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
        
        JSONSchema<User> baseSchema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);
        assertNotNull(baseSchema2.getSchema());
        assertEquals(baseSchema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Json);
    }
}

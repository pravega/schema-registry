/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.avro.schemas;

import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.avro.testobjs.SchemaDefinitions;
import io.pravega.schemaregistry.shared.testobjs.User;
import io.pravega.schemaregistry.avro.testobjs.generated.Test1;
import io.pravega.schemaregistry.avro.testobjs.generated.Test2;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SchemasTest {
    @Test
    public void testAvroSchema() {
        AvroSchema<Object> schema = AvroSchema.of(SchemaDefinitions.SCHEMA1);
        assertNotNull(schema.getSchema());
        assertEquals(schema.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<User> schema2 = AvroSchema.of(User.class);
        assertNotNull(schema2.getSchema());
        assertEquals(schema2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);
        assertNotNull(schema3.getSchema());
        assertEquals(schema3.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<SpecificRecordBase> schemabase1 = AvroSchema.ofSpecificRecord(Test1.class);
        assertNotNull(schemabase1.getSchema());
        assertEquals(schemabase1.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);

        AvroSchema<SpecificRecordBase> schemabase2 = AvroSchema.ofSpecificRecord(Test2.class);
        assertNotNull(schemabase2.getSchema());
        assertEquals(schemabase2.getSchemaInfo().getSerializationFormat(), SerializationFormat.Avro);
    }
}

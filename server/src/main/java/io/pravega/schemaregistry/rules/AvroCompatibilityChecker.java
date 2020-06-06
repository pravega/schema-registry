/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.rules;

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Compatibility checker for Avro schemas. 
 */
public class AvroCompatibilityChecker implements CompatibilityChecker {
    private static final SchemaValidator CAN_READ = new SchemaValidatorBuilder().canReadStrategy().validateAll();
    private static final SchemaValidator CAN_BE_READ = new SchemaValidatorBuilder().canBeReadStrategy().validateAll();
    private static final SchemaValidator MUTUAL_READ = new SchemaValidatorBuilder().mutualReadStrategy().validateAll();

    public boolean canRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        Preconditions.checkArgument(toValidate.getSerializationFormat().equals(SerializationFormat.Avro));
        Preconditions.checkArgument(toValidateAgainst.stream().allMatch(x -> x.getSerializationFormat().equals(SerializationFormat.Avro)));
        Schema schema = new Schema.Parser().parse(new String(toValidate.getSchemaData().array(), Charsets.UTF_8));
        List<Schema> writers = toValidateAgainst.stream().map(x -> new Schema.Parser().parse(new String(x.getSchemaData().array(), Charsets.UTF_8)))
                                                .collect(Collectors.toList());
        try {
            CAN_READ.validate(schema, writers);
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }

    public boolean canBeRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        Preconditions.checkArgument(toValidate.getSerializationFormat().equals(SerializationFormat.Avro));
        Preconditions.checkArgument(toValidateAgainst.stream().allMatch(x -> x.getSerializationFormat().equals(SerializationFormat.Avro)));

        Schema schema = new Schema.Parser().parse(new String(toValidate.getSchemaData().array(), Charsets.UTF_8));
        List<Schema> readers = toValidateAgainst.stream().map(x -> new Schema.Parser().parse(new String(x.getSchemaData().array(), Charsets.UTF_8)))
                                                .collect(Collectors.toList());
        try {
            CAN_BE_READ.validate(schema, readers);
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }

    public boolean canMutuallyRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        Preconditions.checkArgument(toValidate.getSerializationFormat().equals(SerializationFormat.Avro));
        Preconditions.checkArgument(toValidateAgainst.stream().allMatch(s -> s.getSerializationFormat().equals(SerializationFormat.Avro)));

        Schema schema = new Schema.Parser().parse(new String(toValidate.getSchemaData().array(), Charsets.UTF_8));
        List<Schema> schemas = toValidateAgainst.stream().map(x -> new Schema.Parser().parse(new String(x.getSchemaData().array(), Charsets.UTF_8)))
                                                .collect(Collectors.toList());
        try {
            MUTUAL_READ.validate(schema, schemas);
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }
}

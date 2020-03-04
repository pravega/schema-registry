/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Different configuration choices for a group. 
 * 
 * {@link GroupProperties#schemaType} identifies the serialization format and schema type used to describe the schema.
 * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
 * {@link GroupProperties#validateByObjectType} that specifies if schemas have validation rules applied for schemas that share the 
 * same {@link SchemaInfo#name} which represents the object type. This ensures that the registry can support scenarios like 
 * event sourcing, or message bus where different types of events could be written to the same
 * stream. The users can register new versions of each distinct type of schema, and the registry will check for compatibility 
 * for each type independently.
 * If validateByObjectType is set to true, then schemas are validate against other schemas in the group that share the same 
 * {@link SchemaInfo#name}.  
 * {@link GroupProperties#enableEncoding} This flag is used to specify if registry should generate encoding ids or not. 
 * If enableEncoding is set to true, the registry service will generate {@link EncodingId} for writer applications using 
 * registry client to associate encoding id with the serialized payload. A reader application can query the registry service
 * to learn if it needs to expect event payload with an encoding header or not. 
 * This allows schema registry to be used with pravega streams without actually forcing applications to include encoding information 
 * with the payload. This is useful for serialization formats where it is not mandatory to share writer schema at the read time 
 * while still accruing other benefits of registry service such as declaration of structure of data in the stream and evolution
 * of schemas in conformance with schema validation rules. 
 */
@Data
@Builder
@AllArgsConstructor
public class GroupProperties {
    public static final Serializer SERIALIZER = new Serializer();

    private final SchemaType schemaType;
    private final SchemaValidationRules schemaValidationRules;
    private final boolean validateByObjectType;
    private final boolean enableEncoding;

    private static class GroupPropertiesBuilder implements ObjectBuilder<GroupProperties> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<GroupProperties, GroupProperties.GroupPropertiesBuilder> {
        @Override
        protected GroupProperties.GroupPropertiesBuilder newBuilder() {
            return GroupProperties.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(GroupProperties e, RevisionDataOutput target) throws IOException {
            SchemaType.SERIALIZER.serialize(target, e.schemaType);
            SchemaValidationRules.SERIALIZER.serialize(target, e.schemaValidationRules);
            target.writeBoolean(e.validateByObjectType);
            target.writeBoolean(e.enableEncoding);
        }

        private void read00(RevisionDataInput source, GroupProperties.GroupPropertiesBuilder b) throws IOException {
            b.schemaType(SchemaType.SERIALIZER.deserialize(source))
             .schemaValidationRules(SchemaValidationRules.SERIALIZER.deserialize(source))
             .validateByObjectType(source.readBoolean())
             .enableEncoding(source.readBoolean());
        }
    }
}

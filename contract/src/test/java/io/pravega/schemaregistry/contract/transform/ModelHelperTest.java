/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionAndRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import org.junit.Test;

import java.util.Collections;

public class ModelHelperTest {
    @Test
    public void testDecode() {
        SchemaType type = new SchemaType().schemaType(SchemaType.SchemaTypeEnum.CUSTOM).customTypeName("a");
        SchemaValidationRules rules = new SchemaValidationRules().rules(Collections.emptyMap());
        SchemaInfo schema = new SchemaInfo()
                .schemaName("a").schemaType(type).schemaData(new byte[0]).properties(Collections.emptyMap());
        VersionInfo version = new VersionInfo().schemaName("a").version(1);
        Compatibility compatibility = new Compatibility().name(Compatibility.class.getSimpleName())
                                                         .policy(Compatibility.PolicyEnum.BACKWARDANDFORWARDTILL).backwardTill(version);
        CodecType codec = new CodecType().codecType(CodecType.CodecTypeEnum.CUSTOM).customTypeName("a");
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(schema);
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = ModelHelper.decode(type);
        io.pravega.schemaregistry.contract.data.Compatibility compatibilityDecoded = ModelHelper.decode(compatibility);
        io.pravega.schemaregistry.contract.data.CodecType codecType = ModelHelper.decode(new CodecType().codecType(CodecType.CodecTypeEnum.CUSTOM).customTypeName("custom"));
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(version);
        io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo = ModelHelper.decode(new EncodingInfo().schemaInfo(schema).versionInfo(version).codecType(codec));
        io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion = ModelHelper.decode(new SchemaWithVersion().schemaInfo(schema).version(version));
        io.pravega.schemaregistry.contract.data.EncodingId encodingId = ModelHelper.decode(new EncodingId().encodingId(1));
    }

    @Test
    public void testEncode() {
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = io.pravega.schemaregistry.contract.data.SchemaType.custom("custom");
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.data.SchemaInfo(
                "name", schemaType, new byte[0], Collections.emptyMap());
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0);
        io.pravega.schemaregistry.contract.data.Compatibility rule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardTill(
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0),
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 1));
        io.pravega.schemaregistry.contract.data.SchemaValidationRules schemaValidationRules = io.pravega.schemaregistry.contract.data.SchemaValidationRules.of(rule);
        io.pravega.schemaregistry.contract.data.GroupProperties prop = new io.pravega.schemaregistry.contract.data.GroupProperties(
                schemaType, schemaValidationRules, true, Collections.emptyMap());
        io.pravega.schemaregistry.contract.data.CodecType codecType = io.pravega.schemaregistry.contract.data.CodecType.custom("codec", Collections.emptyMap());

        SchemaVersionAndRules schemaEvolution = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.SchemaEvolution(
                schemaInfo, versionInfo, schemaValidationRules));

        SchemaValidationRules rules = ModelHelper.encode(schemaValidationRules);
        
        Compatibility compatibility = ModelHelper.encode(rule);
        SchemaWithVersion schemaWithVersion = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.SchemaWithVersion(schemaInfo, versionInfo));

        GroupProperties groupProperties = ModelHelper.encode(prop);
        
        groupProperties = ModelHelper.encode("groupName", prop);
        
        VersionInfo version = ModelHelper.encode(versionInfo);
        SchemaInfo schema = ModelHelper.encode(schemaInfo);
        SchemaType type = ModelHelper.encode(schemaType);
        EncodingId encodingId = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingId(0));
        CodecType codec = ModelHelper.encode(codecType);
        EncodingInfo encodingInfo = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingInfo(versionInfo, schemaInfo, codecType));
    }

}

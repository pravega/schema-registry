/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class ModelHelperTest {
    @Test
    public void testDecode() {
        SchemaType type = new SchemaType().schemaType(SchemaType.SchemaTypeEnum.CUSTOM).customTypeName("a");
        SchemaValidationRules rules = new SchemaValidationRules().rules(Collections.emptyMap());
        SchemaInfo schema = new SchemaInfo()
                .schemaName("a").schemaType(type).schemaData(new byte[0]).properties(Collections.emptyMap());
        VersionInfo version = new VersionInfo().schemaName("a").version(1).ordinal(1);
        Compatibility compatibility = new Compatibility().name(Compatibility.class.getSimpleName())
                                                         .policy(Compatibility.PolicyEnum.BACKWARDANDFORWARDTILL).backwardTill(version).forwardTill(version);
        CodecType codec = new CodecType().codecType(CodecType.CodecTypeEnum.CUSTOM).customTypeName("custom");
        
        // decodes
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = ModelHelper.decode(type);
        assertEquals(schemaType, io.pravega.schemaregistry.contract.data.SchemaType.Custom);
        assertEquals(schemaType.getCustomTypeName(), "a");

        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(schema);
        assertEquals(schemaInfo.getName(), "a");
        assertEquals(schemaInfo.getSchemaType(), schemaType);
        assertNotNull(schemaInfo.getSchemaData());
        assertNotNull(schemaInfo.getProperties());

        io.pravega.schemaregistry.contract.data.Compatibility compatibilityDecoded = ModelHelper.decode(compatibility);
        assertEquals(compatibilityDecoded.getCompatibility(), io.pravega.schemaregistry.contract.data.Compatibility.Type.BackwardAndForwardTill);
        
        io.pravega.schemaregistry.contract.data.SchemaValidationRules rulesDecoded = ModelHelper.decode(rules);
        assertEquals(rulesDecoded.getRules().size(), 0);
        
        io.pravega.schemaregistry.contract.data.CodecType codecType = ModelHelper.decode(new CodecType().codecType(CodecType.CodecTypeEnum.CUSTOM).customTypeName("custom"));
        assertEquals(codecType, io.pravega.schemaregistry.contract.data.CodecType.Custom);
        assertEquals(codecType.getCustomTypeName(), "custom");

        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(version);
        assertEquals(versionInfo.getSchemaName(), version.getSchemaName());
        assertEquals(versionInfo.getVersion(), version.getVersion().intValue());
        
        io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo = ModelHelper.decode(new EncodingInfo().schemaInfo(schema).versionInfo(version).codecType(codec));
        assertEquals(encodingInfo.getCodec(), io.pravega.schemaregistry.contract.data.CodecType.Custom);
        assertEquals(encodingInfo.getVersionInfo(), versionInfo);
        assertEquals(encodingInfo.getSchemaInfo(), schemaInfo);
        io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion = ModelHelper.decode(new SchemaWithVersion().schemaInfo(schema).version(version));
        assertEquals(schemaWithVersion.getVersion(), versionInfo);
        assertEquals(schemaWithVersion.getSchema(), schemaInfo);

        io.pravega.schemaregistry.contract.data.EncodingId encodingId = ModelHelper.decode(new EncodingId().encodingId(1));
        assertEquals(encodingId.getId(), 1);
    }

    @Test
    public void testEncode() {
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = io.pravega.schemaregistry.contract.data.SchemaType.custom("custom");
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.data.SchemaInfo(
                "name", schemaType, new byte[0], Collections.emptyMap());
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0, 1);
        io.pravega.schemaregistry.contract.data.Compatibility rule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardTill(
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0, 0),
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 1, 1));
        io.pravega.schemaregistry.contract.data.SchemaValidationRules schemaValidationRules = io.pravega.schemaregistry.contract.data.SchemaValidationRules.of(rule);
        io.pravega.schemaregistry.contract.data.GroupProperties prop = io.pravega.schemaregistry.contract.data.GroupProperties.builder()
                                                                                                                              .schemaType(schemaType).schemaValidationRules(schemaValidationRules).versionedBySchemaName(true).properties(Collections.emptyMap()).build();
        io.pravega.schemaregistry.contract.data.CodecType codecType = io.pravega.schemaregistry.contract.data.CodecType.custom("codec", Collections.emptyMap());

        // encode test
        VersionInfo version = ModelHelper.encode(versionInfo);
        assertEquals(version.getVersion().intValue(), versionInfo.getVersion());
        assertEquals(version.getSchemaName(), versionInfo.getSchemaName());
        
        SchemaType type = ModelHelper.encode(schemaType);
        assertEquals(type.getSchemaType(), SchemaType.SchemaTypeEnum.CUSTOM);
        
        SchemaInfo schema = ModelHelper.encode(schemaInfo);
        assertEquals(schema.getSchemaName(), schemaInfo.getName());
        assertEquals(schema.getProperties(), schemaInfo.getProperties());
        assertTrue(Arrays.equals(schema.getSchemaData(), schemaInfo.getSchemaData()));
        assertEquals(schema.getSchemaType(), type);
        
        EncodingId encodingId = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingId(0));
        assertEquals(encodingId.getEncodingId().intValue(), 0);
        
        CodecType codec = ModelHelper.encode(codecType);
        assertEquals(codec.getCodecType(), CodecType.CodecTypeEnum.CUSTOM);
        
        EncodingInfo encodingInfo = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingInfo(versionInfo, schemaInfo, codecType));
        assertEquals(encodingInfo.getCodecType(), codec);
        assertEquals(encodingInfo.getVersionInfo(), version);
        assertEquals(encodingInfo.getSchemaInfo(), schema);

        SchemaValidationRules rules = ModelHelper.encode(schemaValidationRules);
        assertEquals(rules.getRules().size(), 1);
        
        io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord schemaEvolution = ModelHelper.encode(new GroupHistoryRecord(
                schemaInfo, versionInfo, schemaValidationRules, 100L, ""));
        assertEquals(schemaEvolution.getSchemaInfo(), schema);
        assertEquals(schemaEvolution.getValidationRules(), rules);
        assertEquals(schemaEvolution.getVersion(), version);
        assertEquals(schemaEvolution.getTimestamp().longValue(), 100L);
        assertEquals(schemaEvolution.getSchemaString(), "");

        Compatibility compatibility = ModelHelper.encode(rule);
        assertEquals(compatibility.getPolicy(), Compatibility.PolicyEnum.BACKWARDANDFORWARDTILL);
        
        SchemaWithVersion schemaWithVersion = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.SchemaWithVersion(schemaInfo, versionInfo));
        assertEquals(schemaWithVersion.getSchemaInfo(), schema);
        assertEquals(schemaWithVersion.getVersion(), version);
        
        GroupProperties groupProperties = ModelHelper.encode(prop);
        assertEquals(groupProperties.getSchemaType(), type);
        assertEquals(groupProperties.getSchemaValidationRules(), rules);
        assertEquals(groupProperties.isVersionedBySchemaName(), prop.isVersionedBySchemaName());
        assertEquals(groupProperties.getProperties(), prop.getProperties());
    }

}

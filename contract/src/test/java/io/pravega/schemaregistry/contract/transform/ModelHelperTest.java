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

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class ModelHelperTest {
    @Test
    public void testDecode() {
        SerializationFormat type = new SerializationFormat().serializationFormat(SerializationFormat.SerializationFormatEnum.CUSTOM).customTypeName("a");
        SchemaValidationRules rules = new SchemaValidationRules().rules(Collections.emptyMap());
        SchemaInfo schema = new SchemaInfo()
                .type("a").serializationFormat(type).schemaData(new byte[0]).properties(Collections.emptyMap());
        VersionInfo version = new VersionInfo().type("a").version(1).ordinal(1);
        Compatibility compatibility = new Compatibility().name(Compatibility.class.getSimpleName())
                                                         .policy(Compatibility.PolicyEnum.BACKWARDANDFORWARDTILL).backwardTill(version).forwardTill(version);
        String codecType = "custom";

        // decodes
        io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat = ModelHelper.decode(type);
        assertEquals(serializationFormat, io.pravega.schemaregistry.contract.data.SerializationFormat.Custom);
        assertEquals(serializationFormat.getCustomTypeName(), "a");

        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(schema);
        assertEquals(schemaInfo.getType(), "a");
        assertEquals(schemaInfo.getSerializationFormat(), serializationFormat);
        assertNotNull(schemaInfo.getSchemaData());
        assertNotNull(schemaInfo.getProperties());

        io.pravega.schemaregistry.contract.data.Compatibility compatibilityDecoded = ModelHelper.decode(compatibility);
        assertEquals(compatibilityDecoded.getCompatibility(), io.pravega.schemaregistry.contract.data.Compatibility.Type.BackwardAndForwardTill);

        io.pravega.schemaregistry.contract.data.SchemaValidationRules rulesDecoded = ModelHelper.decode(rules);
        assertEquals(rulesDecoded.getRules().size(), 0);
        
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(version);
        assertEquals(versionInfo.getType(), version.getType());
        assertEquals(versionInfo.getVersion(), version.getVersion().intValue());

        io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo = ModelHelper.decode(new EncodingInfo().schemaInfo(schema).versionInfo(version).codecType(codecType));
        assertEquals(encodingInfo.getCodecType(), "custom");
        assertEquals(encodingInfo.getVersionInfo(), versionInfo);
        assertEquals(encodingInfo.getSchemaInfo(), schemaInfo);
        io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion = ModelHelper.decode(new SchemaWithVersion().schemaInfo(schema).version(version));
        assertEquals(schemaWithVersion.getVersionInfo(), versionInfo);
        assertEquals(schemaWithVersion.getSchemaInfo(), schemaInfo);

        io.pravega.schemaregistry.contract.data.EncodingId encodingId = ModelHelper.decode(new EncodingId().encodingId(1));
        assertEquals(encodingId.getId(), 1);
    }

    @Test
    public void testEncode() {
        io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat = io.pravega.schemaregistry.contract.data.SerializationFormat.custom("custom");
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.data.SchemaInfo(
                "name", serializationFormat, ByteBuffer.wrap(new byte[0]), ImmutableMap.of());
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0, 1);
        io.pravega.schemaregistry.contract.data.Compatibility rule = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardTill(
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0, 0),
                new io.pravega.schemaregistry.contract.data.VersionInfo("a", 1, 1));
        io.pravega.schemaregistry.contract.data.SchemaValidationRules schemaValidationRules = io.pravega.schemaregistry.contract.data.SchemaValidationRules.of(rule);
        io.pravega.schemaregistry.contract.data.GroupProperties prop = io.pravega.schemaregistry.contract.data.GroupProperties
                .builder().serializationFormat(serializationFormat).schemaValidationRules(schemaValidationRules)
                .allowMultipleTypes(true).properties(ImmutableMap.of()).build();
        String codecType = "codecType";

        // encode test
        VersionInfo version = ModelHelper.encode(versionInfo);
        assertEquals(version.getVersion().intValue(), versionInfo.getVersion());
        assertEquals(version.getType(), versionInfo.getType());

        SerializationFormat type = ModelHelper.encode(serializationFormat);
        assertEquals(type.getSerializationFormat(), SerializationFormat.SerializationFormatEnum.CUSTOM);

        SchemaInfo schema = ModelHelper.encode(schemaInfo);
        assertEquals(schema.getType(), schemaInfo.getType());
        assertEquals(schema.getProperties(), schemaInfo.getProperties());
        assertTrue(Arrays.equals(schema.getSchemaData(), schemaInfo.getSchemaData().array()));
        assertEquals(schema.getSerializationFormat(), type);

        EncodingId encodingId = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingId(0));
        assertEquals(encodingId.getEncodingId().intValue(), 0);
        
        EncodingInfo encodingInfo = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingInfo(versionInfo, schemaInfo, codecType));
        assertEquals(encodingInfo.getCodecType(), codecType);
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
        assertEquals(groupProperties.getSerializationFormat(), type);
        assertEquals(groupProperties.getSchemaValidationRules(), rules);
        assertEquals(groupProperties.isAllowMultipleTypes(), prop.isAllowMultipleTypes());
        assertEquals(groupProperties.getProperties(), prop.getProperties());
    }

}

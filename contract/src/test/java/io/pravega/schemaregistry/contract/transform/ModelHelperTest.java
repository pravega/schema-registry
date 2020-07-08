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
import io.pravega.schemaregistry.contract.generated.rest.model.Backward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardAndForward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardPolicy;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardTill;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.ForwardPolicy;
import io.pravega.schemaregistry.contract.generated.rest.model.ForwardTill;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
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
        SerializationFormat type = new SerializationFormat().serializationFormat(SerializationFormat.SerializationFormatEnum.CUSTOM).fullTypeName("a");
        Compatibility backward = new Compatibility()
                .policy(Compatibility.PolicyEnum.ADVANCED)
                .advanced(new BackwardAndForward().backwardPolicy(new BackwardPolicy()
                        .backwardPolicy(new Backward().name(Backward.class.getSimpleName()))));
        SchemaInfo schema = new SchemaInfo()
                .type("a").serializationFormat(type).schemaData(new byte[0]).properties(Collections.emptyMap());
        VersionInfo version = new VersionInfo().type("a").version(1).id(1);
        Compatibility backwardTillForwardTill = new Compatibility()
                .policy(Compatibility.PolicyEnum.ADVANCED)
                .advanced(new BackwardAndForward().backwardPolicy(new BackwardPolicy()
                        .backwardPolicy(new BackwardTill().name(BackwardTill.class.getSimpleName()).versionInfo(version)))
                                                       .forwardPolicy(new ForwardPolicy().forwardPolicy(new ForwardTill().name(ForwardTill.class.getSimpleName()).versionInfo(version)))
                );
        CodecType codecType = new CodecType().name("custom");

        // decodes
        io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat = ModelHelper.decode(type);
        assertEquals(serializationFormat, io.pravega.schemaregistry.contract.data.SerializationFormat.Custom);
        assertEquals(serializationFormat.getFullTypeName(), "a");

        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(schema);
        assertEquals(schemaInfo.getType(), "a");
        assertEquals(schemaInfo.getSerializationFormat(), serializationFormat);
        assertNotNull(schemaInfo.getSchemaData());
        assertNotNull(schemaInfo.getProperties());

        io.pravega.schemaregistry.contract.data.Compatibility compatibilityDecoded = ModelHelper.decode(backwardTillForwardTill);
        assertNotNull(compatibilityDecoded.getBackwardAndForward());
        io.pravega.schemaregistry.contract.data.BackwardAndForward backwardAndForward =
                compatibilityDecoded.getBackwardAndForward();
        assertTrue(backwardAndForward.getBackwardPolicy() instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill);
        assertTrue(backwardAndForward.getForwardPolicy() instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill);

        io.pravega.schemaregistry.contract.data.Compatibility backwardDecoded = ModelHelper.decode(backwardTillForwardTill);
        assertNotNull(backwardDecoded.getBackwardAndForward());
        io.pravega.schemaregistry.contract.data.BackwardAndForward backwardAndForwardDecoded =
                backwardDecoded.getBackwardAndForward();
        assertTrue(backwardAndForwardDecoded.getBackwardPolicy() instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill);
        assertTrue(backwardAndForwardDecoded.getForwardPolicy() instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill);

        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(version);
        assertEquals(versionInfo.getType(), version.getType());
        assertEquals(versionInfo.getVersion(), version.getVersion().intValue());

        io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo = ModelHelper.decode(
                new EncodingInfo().schemaInfo(schema).versionInfo(version).codecType(codecType));
        assertEquals(encodingInfo.getCodecType().getName(), "custom");
        assertEquals(encodingInfo.getVersionInfo(), versionInfo);
        assertEquals(encodingInfo.getSchemaInfo(), schemaInfo);
        io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion = ModelHelper.decode(new SchemaWithVersion().schemaInfo(schema).versionInfo(version));
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
        io.pravega.schemaregistry.contract.data.Compatibility compatibility = io.pravega.schemaregistry.contract.data.Compatibility
                .backwardTillAndForwardTill(
                        new io.pravega.schemaregistry.contract.data.VersionInfo("a", 0, 0),
                        new io.pravega.schemaregistry.contract.data.VersionInfo("a", 1, 1));

        io.pravega.schemaregistry.contract.data.GroupProperties prop = io.pravega.schemaregistry.contract.data.GroupProperties
                .builder().serializationFormat(serializationFormat).compatibility(compatibility)
                .allowMultipleTypes(true).properties(ImmutableMap.of()).build();
        io.pravega.schemaregistry.contract.data.CodecType codecType = new io.pravega.schemaregistry.contract.data.CodecType("codecType");

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

        EncodingInfo encodingInfo = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.EncodingInfo(
                versionInfo, schemaInfo, codecType));
        assertEquals(encodingInfo.getCodecType(), ModelHelper.encode(codecType));
        assertEquals(encodingInfo.getVersionInfo(), version);
        assertEquals(encodingInfo.getSchemaInfo(), schema);

        Compatibility rules = ModelHelper.encode(compatibility);
        assertEquals(rules.getPolicy(), Compatibility.PolicyEnum.ADVANCED);
        assertTrue(rules.getAdvanced().getBackwardPolicy().getBackwardPolicy() instanceof BackwardTill);
        assertTrue(rules.getAdvanced().getForwardPolicy().getForwardPolicy() instanceof ForwardTill);

        GroupHistoryRecord schemaEvolution = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.GroupHistoryRecord(
                schemaInfo, versionInfo, compatibility, 100L, ""));
        assertEquals(schemaEvolution.getSchemaInfo(), schema);
        assertEquals(schemaEvolution.getCompatibility(), rules);
        assertEquals(schemaEvolution.getVersionInfo(), version);
        assertEquals(schemaEvolution.getTimestamp().longValue(), 100L);
        assertEquals(schemaEvolution.getSchemaString(), "");
        
        SchemaWithVersion schemaWithVersion = ModelHelper.encode(new io.pravega.schemaregistry.contract.data.SchemaWithVersion(schemaInfo, versionInfo));
        assertEquals(schemaWithVersion.getSchemaInfo(), schema);
        assertEquals(schemaWithVersion.getVersionInfo(), version);

        GroupProperties groupProperties = ModelHelper.encode(prop);
        assertEquals(groupProperties.getSerializationFormat(), type);
        assertEquals(groupProperties.getCompatibility(), rules);
        assertEquals(groupProperties.isAllowMultipleTypes(), prop.isAllowMultipleTypes());
        assertEquals(groupProperties.getProperties(), prop.getProperties());
    }

    @Test
    public void testEncodeAndDecodeCompatibility() {
        io.pravega.schemaregistry.contract.data.Compatibility compatibility = 
                io.pravega.schemaregistry.contract.data.Compatibility.allowAny();
        Compatibility encoded = ModelHelper.encode(compatibility);
        io.pravega.schemaregistry.contract.data.Compatibility decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.denyAll();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backward();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.forward();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backwardTransitive();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.forwardTransitive();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.full();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.fullTransitive();
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = new io.pravega.schemaregistry.contract.data.VersionInfo("a", 1, 1);
        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backwardTill(versionInfo);
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.forwardTill(versionInfo);
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardTill(versionInfo, versionInfo);
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backwardOneAndForwardTill(versionInfo);
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

        compatibility = io.pravega.schemaregistry.contract.data.Compatibility.backwardTillAndForwardOne(versionInfo);
        encoded = ModelHelper.encode(compatibility);
        decoded = ModelHelper.decode(encoded);
        assertEquals(compatibility, decoded);

    }
}

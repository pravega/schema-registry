/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.cache;

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CacheTest {
    @Test
    public void testCache() {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        String groupId = "groupId";
        EncodingId encodingId = new EncodingId(0);
        EncodingInfo encodingInfo = new EncodingInfo(new VersionInfo("name", 0, 0),
                new SchemaInfo("name", SchemaType.Avro, new byte[0], Collections.emptyMap()), CodecFactory.snappy().getCodecType());
        doAnswer(x -> encodingInfo).when(client).getEncodingInfo(eq(groupId), eq(encodingId));
        EncodingCache cache = EncodingCache.getEncodingCacheForGroup(groupId, client);
        assertEquals(encodingInfo, cache.getGroupEncodingInfo(encodingId));
    }
}

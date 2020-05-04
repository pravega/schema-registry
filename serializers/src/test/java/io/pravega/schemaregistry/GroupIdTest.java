/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GroupIdTest {
    @Test
    public void testGroupId() throws UnsupportedEncodingException {
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, "scope", "stream");
        
        assertTrue(groupId.startsWith("pravega"));
        assertEquals(URLDecoder.decode(groupId, Charsets.UTF_8.toString()), "pravega://scope/stream/");
    }
}

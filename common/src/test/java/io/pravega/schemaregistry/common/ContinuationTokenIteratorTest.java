/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.common;

import com.google.common.collect.Lists;
import lombok.Data;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ContinuationTokenIteratorTest {
    @Test
    public void test() {
        Queue<ListWithToken> responses = spy(new LinkedBlockingQueue<>());
        responses.add(new ListWithToken(Lists.newArrayList(1, 2, 3, 4, 5), "1"));
        responses.add(new ListWithToken(Collections.emptyList(), ""));
        Function<String, Map.Entry<String, Collection<Integer>>> func = token -> {
            ListWithToken result = responses.poll();
            return new AbstractMap.SimpleEntry<>(result.token, result.list);
        };
        ContinuationTokenIterator<Integer, String> myIterator = new ContinuationTokenIterator<>(func, null);
        for (int i = 0; i < 5; i++) {
            assertTrue(myIterator.hasNext());
            assertEquals(myIterator.next().intValue(), i + 1);
        }
        verify(responses, times(1)).poll();
        assertFalse(myIterator.hasNext());
    }
    
    @Data
    static class ListWithToken {
        private final List<Integer> list;
        private final String token;
    }
}

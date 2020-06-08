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
        // 1. call method 1st call returns - list of 5 items + new token
        // verify that call method is not called until all 10 are read. 
        // 2. call returns empty list + new token
        // 3. call returns empty list + new token
        // 4. call returns list of 10 items + new token
        // verify that we consume 10 items without calling the callmethod
        // 5. call returns empty list  + same token. --> this should exit
        Queue<ListWithToken> responses = spy(new LinkedBlockingQueue<>());
        responses.add(new ListWithToken(Lists.newArrayList(1, 2, 3, 4, 5), "1"));
        responses.add(new ListWithToken(Collections.emptyList(), "2"));
        responses.add(new ListWithToken(Collections.emptyList(), "3"));
        responses.add(new ListWithToken(Lists.newArrayList(6, 7, 8, 9, 10), "4"));
        responses.add(new ListWithToken(Collections.emptyList(), "4"));
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
        for (int i = 5; i < 10; i++) {
            assertTrue(myIterator.hasNext());
            assertEquals(myIterator.next().intValue(), i + 1);
        }
        verify(responses, times(4)).poll();
        assertFalse(myIterator.hasNext());
        verify(responses, times(5)).poll();
    }
    
    @Data
    static class ListWithToken {
        private final List<Integer> list;
        private final String token;
    }
}

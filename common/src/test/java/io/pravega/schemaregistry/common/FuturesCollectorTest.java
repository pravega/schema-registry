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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FuturesCollectorTest {

    private ScheduledExecutorService executor;
    @Before
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }
    
    @After
    public void tearDown() {
        executor.shutdownNow();    
    }
    
    @Test
    public void testFilteredWithTokenAndLimit() {
        List<Integer> list = Lists.newArrayList(0, 1, 2, 0, 0, 3, 0, 4, 5, 6, 7, 0, 0, 0, 0, 8, 9, 10, 11, 0, 0, 0, 12, 0, 13);
        BiFunction<Integer, Integer, CompletableFuture<Map.Entry<Integer, List<Integer>>>> fn = (x, y) -> {
            if (x > list.size()) {
                return CompletableFuture.completedFuture(new AbstractMap.SimpleEntry<>(x, Collections.emptyList()));
            }
            return CompletableFuture.completedFuture(new AbstractMap.SimpleEntry<>(Math.min(x + y, list.size() + 1), list.subList(x, Math.min(x + y, list.size()))));
        };

        Map.Entry<Integer, List<Integer>> result = FuturesCollector.filteredWithTokenAndLimit(fn, x -> x != 0, 0, 10, executor).join();
        assertEquals(result.getValue().size(), 10);
        assertTrue(result.getValue().stream().noneMatch(x -> x == 0));
        assertEquals(result.getKey().intValue(), list.indexOf(11));

        result = FuturesCollector.filteredWithTokenAndLimit(fn, x -> x != 0, result.getKey(), 10, executor).join();
        assertEquals(result.getValue().size(), 3);
        assertTrue(result.getValue().stream().noneMatch(x -> x == 0));
        assertEquals(result.getKey().intValue(), list.size() + 1);
    }
}

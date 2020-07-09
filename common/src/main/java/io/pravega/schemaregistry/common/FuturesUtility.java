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

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A collector utility that has methods to collect records by invoking the supplied future.  
 */
public class FuturesUtility {
    /**
     * This method invokes the supplied retrieveFunction with continuation token and limit. 
     * The `filter` predicate is then applied on the received list of response. 
     * If values from the response are filtered out and there could be more data available to be read, it invokes
     * the retrieve function again until either it has collected `limit` number of responses OR there are no
     * additional responses remaining. 
     * 
     * @param retrieveFunction A function that when invoked, will retrieve a batch of records from the supplied continuation token,
     *                         upto the limit specified in the request. 
     * @param filter A predicate that is used to filter and include only those responses that satisfy the predicate.
     * @param continuationToken continuation token from which to read.
     * @param limit number of elements to read
     * @param executorService executor where the execution should be performed. 
     * @param <T> Type of record.
     * @param <C> Type of continuation token
     * @return returns a completablefuture which when completed will contain a pair of filtered responses from retrieveFunction limited
     * to number of elements as specified by the limit parameter AND the continuation token which can be used to read
     * after the returned result. 
     */
    public static <T, C> CompletableFuture<Map.Entry<C, List<T>>> filteredWithTokenAndLimit(
            BiFunction<C, Integer, CompletableFuture<Map.Entry<C, List<T>>>> retrieveFunction,
            Predicate<T> filter, C continuationToken, int limit, Executor executorService) {
        Preconditions.checkNotNull(retrieveFunction, "Retrieve function cannot be null");
        Preconditions.checkNotNull(filter, "filter cannot be null");
        Preconditions.checkNotNull(executorService, "executor cannot be null");
        AtomicBoolean loop = new AtomicBoolean(true);
        AtomicInteger limitRemaining = new AtomicInteger(limit);
        AtomicReference<C> token = new AtomicReference<>(continuationToken);
        List<T> list = new LinkedList<>();

        return Futures.loop(loop::get,
                () -> retrieveFunction.apply(token.get(), limitRemaining.get())
                                     .thenAccept(result -> {
                                         List<T> filtered = result.getValue().stream()
                                                                  .filter(filter)
                                                                  .collect(Collectors.toList());
                                         int filteredOut = result.getValue().size() - filtered.size();
                                         list.addAll(filtered);
                                         loop.set(filteredOut > 0 && result.getValue().size() == limitRemaining.get());
                                         limitRemaining.set(limit - list.size());
                                         token.set(result.getKey());
                                     }), executorService)
                      .thenApply(v -> new AbstractMap.SimpleEntry<>(token.get(), list));
    }
}

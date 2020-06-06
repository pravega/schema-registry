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

import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * Continuation token iterator which fetches a batch of values using the loading function. Once those values have been 
 * iterated over, it uses the continuation token to read more values using the loading function until the function does
 * not return a value. 
 * @param <T> Type of value.
 * @param <Token> Type of continuation token.
 */
public class ContinuationTokenIterator<T, Token> implements Iterator<T> {
    @GuardedBy("$lock")
    private final Queue<T> queue;
    private final Function<Token, Map.Entry<Token, Collection<T>>> loadingFunction;
    @GuardedBy("lock")
    private Token token;
    @GuardedBy("$lock")
    private T next;
    @GuardedBy("$lock")
    private boolean canHaveNext;

    public ContinuationTokenIterator(Function<Token, Map.Entry<Token, Collection<T>>> loadingFunction, Token tokenIdentity) {
        this.loadingFunction = loadingFunction;
        this.queue = new LinkedBlockingQueue<T>();
        this.token = tokenIdentity;
        this.canHaveNext = true;
        this.next = null;
    }

    @Synchronized
    private void load() {
        if (next == null && canHaveNext) {
            Map.Entry<Token, Collection<T>> result = loadingFunction.apply(token);
            token = result.getKey();

            queue.addAll(result.getValue());
            next = queue.poll();
            if (next == null) {
                canHaveNext = false;
            }
        }
    }

    @Synchronized
    @Override
    public boolean hasNext() {
        load();
        return canHaveNext;
    }

    @Synchronized
    @Override
    public T next() {
        load();
        if (next != null) {
            T retVal = next;
            next = null;
            return retVal;
        } else {
            assert !canHaveNext;
            throw new NoSuchElementException();
        }
    }
}

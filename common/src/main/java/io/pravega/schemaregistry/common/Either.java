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
import lombok.Data;

@Data
public class Either<T, K> {
    private final T left;
    private final K right;
    
    private Either(T left, K right) {
        this.left = left;
        this.right = right;
    }

    public static <T, K> Either<T, K> left(T t) {
        Preconditions.checkNotNull(t);
        return new Either<T, K>(t, null);
    }

    public static <T, K> Either<T, K> right(K k) {
        return new Either<T, K>(null, k);
    }
    
    public boolean isLeft() {
        return left != null;
    } 

    public boolean isRight() {
        return right != null;
    } 
}

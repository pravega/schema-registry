/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import lombok.Data;

/**
 * Defines different Compatibility policy options for schema evolution for schemas within a group (or subgroup).

 * {@link Type#AllowAny}: allow any changes to schema without any checks performed by the registry. 
 * {@link Type#DisallowAll}: disables any changes to the schema for the group.
 * {@link Type#Backward}: a new schema can read data written by last schema. 
 * {@link Type#BackwardTransitive}: a new schema can read data written by any of previous schemas. 
 * {@link Type#BackwardTill}: a new schema can read data written by any of previous schemas till schema 
 * identified by version {@link Compatibility#backwardTill}. 
 * {@link Type#Forward}: last schema can read data written by new schema. 
 * {@link Type#ForwardTransitive}: all previous schemas can read data written by new schema. 
 * {@link Type#ForwardTill}: All previous schemas till schema identified by version {@link Compatibility#forwardTill}
 * can read data written by new schema. 
 * {@link Type#Full}: both backward and forward.
 * {@link Type#FullTransitive}: both backward and forward compatibility with all previous schemas.
 * {@link Type#BackwardTillAndForwardTill}: All previous schemas till schema identified by version {@link Compatibility#forwardTill}
 * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
 * identified by version {@link Compatibility#backwardTill}. 
 */
@Data
public class Compatibility {
    private final Type compatibility;
    private final VersionInfo backwardTill;
    private final VersionInfo forwardTill;

    private Compatibility(Type compatibility) {
        this(compatibility, null, null);
    }

    public Compatibility(Type compatibility, VersionInfo backwardTill, VersionInfo forwardTill) {
        this.compatibility = compatibility;
        this.backwardTill = backwardTill;
        this.forwardTill = forwardTill;
    }
    
    public enum Type {
        AllowAny, 
        DisallowAll, 
        Backward,
        BackwardTill,
        BackwardTransitive, 
        Forward, 
        ForwardTill,
        ForwardTransitive,
        BackwardTillAndForwardTill,
        Full,
        FullTransitive;
    }

    public static Compatibility of(Compatibility.Type type) {
        return new Compatibility(type);
    }

    public static Compatibility backwardTill(VersionInfo backwardTill) {
        return new Compatibility(Type.BackwardTill, backwardTill, null);
    }

    public static Compatibility forwardTill(VersionInfo forwardTill) {
        return new Compatibility(Type.ForwardTill, null, forwardTill);
    }

    public static Compatibility backwardTillAndForwardTill(VersionInfo backwardTill, VersionInfo forwardTill) {
        return new Compatibility(Type.ForwardTill, backwardTill, forwardTill);
    }

}

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

import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;

/**
 * Defines different Compatibility policy options for schema evolution for schemas within a group.

 * {@link Type#AllowAny}: allow any changes to schema without any checks performed by the registry. 
 * {@link Type#DenyAll}: disables any changes to the schema for the group.
 * {@link Type#Backward}: a new schema can be used to read data written by previous schema. 
 * {@link Type#BackwardTransitive}: a new schema can be used read data written by any of previous schemas. 
 * {@link Type#BackwardTill}: a new schema can be used to read data written by any of previous schemas till schema 
 * identified by version {@link Compatibility#backwardTill}. 
 * {@link Type#Forward}: previous schema can be used to read data written by new schema. 
 * {@link Type#ForwardTransitive}: all previous schemas can read data written by new schema. 
 * {@link Type#ForwardTill}: All previous schemas till schema identified by version {@link Compatibility#forwardTill}
 * can read data written by new schema. 
 * {@link Type#Full}: both backward and forward compatibility.
 * {@link Type#FullTransitive}: both backward and forward compatibility with all previous schemas.
 * {@link Type#BackwardAndForwardTill}: All previous schemas till schema identified by version {@link Compatibility#forwardTill}
 * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
 * identified by version {@link Compatibility#backwardTill}. 
 */
@Data
@Builder
public class Compatibility implements SchemaValidationRule {
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

    @Override
    public String getName() {
        return Compatibility.class.getSimpleName();
    }

    public enum Type {
        AllowAny,
        DenyAll,
        Backward,
        BackwardTill,
        BackwardTransitive,
        Forward,
        ForwardTill,
        ForwardTransitive,
        BackwardAndForwardTill,
        Full,
        FullTransitive;
    }
    
    public static Compatibility backward() {
        return new Compatibility(Type.Backward);
    }
    
    public static Compatibility backwardTill(VersionInfo version) {
        return new Compatibility(Type.BackwardTill, version, null);
    }
    
    public static Compatibility backwardTransitive() {
        return new Compatibility(Type.BackwardTransitive);
    }
    
    public static Compatibility forward() {
        return new Compatibility(Type.Forward);
    }

    public static Compatibility forwardTill(VersionInfo forwardTill) {
        return new Compatibility(Type.ForwardTill, null, forwardTill);
    }

    public static Compatibility forwardTransitive() {
        return new Compatibility(Type.ForwardTransitive);
    }

    public static Compatibility full() {
        return new Compatibility(Type.Full);
    }

    public static Compatibility fullTransitive() {
        return new Compatibility(Type.FullTransitive);
    }

    public static Compatibility backwardTillAndForwardTill(VersionInfo backwardTill, VersionInfo forwardTill) {
        return new Compatibility(Type.BackwardAndForwardTill, backwardTill, forwardTill);
    }

    public static Compatibility allowAny() {
        return new Compatibility(Type.AllowAny);
    }

    public static Compatibility denyAll() {
        return new Compatibility(Type.DenyAll);
    }

    public static class CompatibilityBuilder implements ObjectBuilder<Compatibility> {
    }
}

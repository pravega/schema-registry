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
 * The choice of compatibility policy tells the Schema Registry service whether a schema should be accepted to evolve
 * into new schema by comparing it with one or more existing versions of the schema. 
 * 
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
    /**
     * Enum that defines the Type of compatibility policy.
     */
    private final Type compatibility;
    /**
     * Version info to be specified if the compatibility policy choic.e is either {@link Type#backwardTill} or 
     * {@link Type#backwardTillAndForwardTill}.
     */
    private final VersionInfo backwardTill;
    /**
     * Version info to be specified if the compatibility policy choice is either {@link Type#forwardTill} or 
     * {@link Type#backwardTillAndForwardTill}.
     */
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

    /**
     * Method to create a compatibility policy of type backward. Backward policy implies new schema will be validated
     * to be capable of reading data written using the previous schema. 
     * 
     * @return Compatibility with Type.Backward.
     */
    public static Compatibility backward() {
        return new Compatibility(Type.Backward);
    }

    /**
     * Method to create a compatibility policy of type backward till. BackwardTill policy implies new schema will be validated
     * to be capable of reading data written using the all previous schemas till version supplied as input.
     * 
     * @param backwardTill version till which schemas should be checked for compatibility.
     * @return Compatibility with Type.BackwardTill version.
     */
    public static Compatibility backwardTill(VersionInfo backwardTill) {
        return new Compatibility(Type.BackwardTill, backwardTill, null);
    }

    /**
     * Method to create a compatibility policy of type backward transitive. Backward transitive policy implies 
     * new schema will be validated to be capable of reading data written using the all previous schemas versions.
     * 
     * @return Compatibility with Type.BackwardTransitive.
     */
    public static Compatibility backwardTransitive() {
        return new Compatibility(Type.BackwardTransitive);
    }

    /**
     * Method to create a compatibility policy of type forward. Forward policy implies new schema will be validated
     * such that data written using new schema can be read using the previous schema. 
     *      
     * @return Compatibility with Type.Forward
     */
    public static Compatibility forward() {
        return new Compatibility(Type.Forward);
    }

    /**
     * Method to create a compatibility policy of type forward till. Forward policy implies new schema will be validated
     * such that data written using new schema can be read using the all previous schemas till supplied version. 
     *
     * @param forwardTill version till which schemas should be checked for compatibility.
     * @return Compatibility with Type.ForwardTill version.
     */
    public static Compatibility forwardTill(VersionInfo forwardTill) {
        return new Compatibility(Type.ForwardTill, null, forwardTill);
    }

    /**
     * Method to create a compatibility policy of type forward transitive. 
     * Forward transitive policy implies new schema will be validated such that data written using new schema 
     * can be read using all previous schemas. 
     *      
     * @return Compatibility with Type.ForwardTransitive.
     */
    public static Compatibility forwardTransitive() {
        return new Compatibility(Type.ForwardTransitive);
    }

    /**
     * Method to create a compatibility policy of type full. Full means backward and forward compatibility check with 
     * previous schema version. Which means new schema can be used to read data written with previous schema and vice versa. 
     * 
     * @return Compatibility with Type.Full.
     */
    public static Compatibility full() {
        return new Compatibility(Type.Full);
    }

    /**
     * Method to create a compatibility policy of type full transitive. 
     * Full transitive means backward and forward compatibility check with all previous schema version. 
     * This implies new schema can be used to read data written with any of the previous schemas and vice versa. 
     *
     * @return Compatibility with Type.FullTransitive.
     */
    public static Compatibility fullTransitive() {
        return new Compatibility(Type.FullTransitive);
    }

    /**
     * Method to create a compatibility policy of type backward till and forward till. This is a combination of  
     * backward till and forward till policies. 
     * All previous schemas till schema identified by version {@link Compatibility#forwardTill}
     * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
     * identified by version {@link Compatibility#backwardTill}. 
     * 
     * @param backwardTill version till which backward compatibility is checked for.
     * @param forwardTill version till which forward compatibility is checked for.
     * @return Compatibility with Type.FullTransitive.
     */
    public static Compatibility backwardTillAndForwardTill(VersionInfo backwardTill, VersionInfo forwardTill) {
        return new Compatibility(Type.BackwardAndForwardTill, backwardTill, forwardTill);
    }

    /**
     * Disable compatibility check and all any schema to be registered. Effectively declares all schemas as compatible.  
     * 
     * @return Compatibility with Type.AllowAny
     */
    public static Compatibility allowAny() {
        return new Compatibility(Type.AllowAny);
    }

    /**
     * Compatibility policy that disallows any new schema changes. Effecfively rejects all schemas and declares them incompatible. 
     *
     * @return Compatibility with Type.DenyAll
     */
    public static Compatibility denyAll() {
        return new Compatibility(Type.DenyAll);
    }

    public static class CompatibilityBuilder implements ObjectBuilder<Compatibility> {
    }
}

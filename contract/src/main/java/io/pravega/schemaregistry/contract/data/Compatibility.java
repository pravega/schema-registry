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

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;

/**
 * Defines different Compatibility policy options for schema evolution for schemas within a group.
 * The choice of compatibility policy tells the Schema Registry service whether a schema should be accepted to evolve
 * into new schema by comparing it with one or more existing versions of the schema. 
 *
 */
@Data
@Builder
public class Compatibility {
    /**
     * Enum that defines the Type of compatibility policy.
     */
    private final Type type;
    private final BackwardAndForward backwardAndForward;

    private Compatibility(Type type) {
        this(type, null);
    }

    private Compatibility(Type type, BackwardAndForward backwardAndForward) {
        Preconditions.checkArgument(!type.equals(Type.BackwardAndForward) || backwardAndForward != null);
        this.type = type;
        this.backwardAndForward = backwardAndForward;
    }

    /**
     * {@link Type#AllowAny}: allow any changes to schema without any checks performed by the registry. 
     * {@link Type#DenyAll}: disables any changes to the schema for the group.
     * {@link Type#BackwardAndForward}: 
     */
    public enum Type {
        AllowAny,
        DenyAll,
        BackwardAndForward,
    }

    /**
     * Disable compatibility check and all any schema to be registered. Effectively declares all schemas as compatible.  
     *
     * @return Compatibility policy that allows any change.
     */
    public static Compatibility allowAny() {
        return new Compatibility(Type.AllowAny);
    }

    /**
     * Compatibility policy that disallows any new schema changes. Effecfively rejects all schemas and declares them incompatible. 
     *
     * @return Compatibility policy that denies all changes. 
     */
    public static Compatibility denyAll() {
        return new Compatibility(Type.DenyAll);
    }

    private static Compatibility backwardAndForward(BackwardAndForward backwardAndForward) {
        return new Compatibility(Type.BackwardAndForward, backwardAndForward);
    }
    
    /**
     * Method to create a compatibility policy of type backwardPolicy. BackwardPolicy policy implies new schema will be validated
     * to be capable of reading data written using the previous schema. 
     *
     * @return Compatibility policy with Backward check.
     */
    public static Compatibility backward() {
        return backwardAndForward(new BackwardAndForward(new BackwardAndForward.Backward(), null));
    }

    /**
     * Method to create a compatibility policy of type backwardPolicy till. BackwardTill policy implies new schema will be validated
     * to be capable of reading data written using the all previous schemas till version supplied as input.
     *
     * @param backwardTill version till which schemas should be checked for compatibility.
     * @return Compatibility policy with BackwardTill version check.
     */
    public static Compatibility backwardTill(VersionInfo backwardTill) {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.BackwardTill(backwardTill), null));
    }

    /**
     * Method to create a compatibility policy of type backwardPolicy transitive. BackwardPolicy transitive policy implies 
     * new schema will be validated to be capable of reading data written using the all previous schemas versions.
     *
     * @return Compatibility policy with BackwardTransitive check.
     */
    public static Compatibility backwardTransitive() {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.BackwardTransitive(), null));
    }

    /**
     * Method to create a compatibility policy of type forward. ForwardPolicy policy implies new schema will be validated
     * such that data written using new schema can be read using the previous schema. 
     *
     * @return Compatibility policy with Forward compatibility check.
     */
    public static Compatibility forward() {
        return Compatibility.backwardAndForward(new BackwardAndForward(null, new BackwardAndForward.Forward()));
    }

    /**
     * Method to create a compatibility policy of type forward till. ForwardPolicy policy implies new schema will be validated
     * such that data written using new schema can be read using the all previous schemas till supplied version. 
     *
     * @param forwardTill version till which schemas should be checked for compatibility.
     * @return Compatibility policy with ForwardTill check.
     */
    public static Compatibility forwardTill(VersionInfo forwardTill) {
        return Compatibility.backwardAndForward(new BackwardAndForward(null, new BackwardAndForward.ForwardTill(forwardTill)));
    }

    /**
     * Method to create a compatibility policy of type forward transitive. 
     * ForwardPolicy transitive policy implies new schema will be validated such that data written using new schema 
     * can be read using all previous schemas. 
     *
     * @return Compatibility policy with ForwardTransitive check.
     */
    public static Compatibility forwardTransitive() {
        return Compatibility.backwardAndForward(new BackwardAndForward(null, new BackwardAndForward.ForwardTransitive()));
    }

    /**
     * Method to create a compatibility policy of type full. Full means backwardPolicy and forward compatibility check with 
     * previous schema version. Which means new schema can be used to read data written with previous schema and vice versa. 
     *
     * @return Compatibility policy with Backward and Forward compatibility checks.
     */
    public static Compatibility full() {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.Backward(), new BackwardAndForward.Forward()));
    }

    /**
     * Method to create a compatibility policy of type full transitive.  
     * Full transitive means backwardPolicy and forward compatibility check with all previous schema version. 
     * This implies new schema can be used to read data written with any of the previous schemas and vice versa. 
     *
     * @return Compatibility policy of type Backward Transitive and Forward Transitive checks.
     */
    public static Compatibility fullTransitive() {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.BackwardTransitive(), new BackwardAndForward.ForwardTransitive()));
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy till and forwardOne till. This is a combination of  
     * backwardPolicy till and forwardOne till policies. 
     * All previous schemas till schema identified by version specified with {@link BackwardAndForward.BackwardTill} policy
     * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
     * identified by version {@link BackwardAndForward.ForwardTill}. 
     *
     * @param backwardTill version till which backwardPolicy schemaValidationRules is checked for.
     * @param forwardTill version till which forwardOne schemaValidationRules is checked for.
     * @return Compatibility policy with backwardTill check And ForwardTill check.
     */
    public static Compatibility backwardTillAndForwardTill(VersionInfo backwardTill, VersionInfo forwardTill) {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.BackwardTill(backwardTill), new BackwardAndForward.ForwardTill(forwardTill)));
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy one and forwardOne till. 
     *
     * All previous schemas till schema identified by version {@link BackwardAndForward.ForwardTill}
     * can read data written by new schema. New schema can be used to read data written by previous schema.
     *
     * @param forwardTill version till which forwardTill schemaValidationRules is checked for.
     * @return Compatibility policy that describes backward check And ForwardTill check.
     */
    public static Compatibility backwardOneAndForwardTill(VersionInfo forwardTill) {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.Backward(), new BackwardAndForward.ForwardTill(forwardTill)));
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy till one and forwardOne one. 
     *
     * All previous schemas till schema identified by version {@link BackwardAndForward.BackwardTill}
     * can read data written by new schema. New schema can be used to read data written by previous schema.
     *
     * @param backwardTill version till which backwardTill schemaValidationRules is checked for.
     * @return BackwardAndForward with backwardTill check And Forward check.
     */
    public static Compatibility backwardTillAndForwardOne(VersionInfo backwardTill) {
        return Compatibility.backwardAndForward(new BackwardAndForward(new BackwardAndForward.BackwardTill(backwardTill), new BackwardAndForward.Forward()));
    }

    public static class CompatibilityBuilder implements ObjectBuilder<Compatibility> {
    }
}

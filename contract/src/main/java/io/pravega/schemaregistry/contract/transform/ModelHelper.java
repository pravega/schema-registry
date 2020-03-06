/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionType;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionAndRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

public class ModelHelper {

    // region decode
    public static io.pravega.schemaregistry.contract.data.SchemaInfo decode(SchemaInfo schemaInfo) {
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new io.pravega.schemaregistry.contract.data.SchemaInfo(schemaInfo.getSchemaName(), schemaType, schemaInfo.getSchemaData(),
                ImmutableMap.copyOf(schemaInfo.getProperties()));
    }

    public static io.pravega.schemaregistry.contract.data.SchemaType decode(SchemaType schemaType) {
        switch (schemaType.getSchemaType()) {
            case CUSTOM:
                return io.pravega.schemaregistry.contract.data.SchemaType.custom(schemaType.getCustomTypeName());
            default:
                return searchEnum(io.pravega.schemaregistry.contract.data.SchemaType.class, schemaType.getSchemaType().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.SchemaValidationRules decode(SchemaValidationRules rules) {
        io.pravega.schemaregistry.contract.data.Compatibility compatibilityRule = decode(rules.getCompatibility());
        return new io.pravega.schemaregistry.contract.data.SchemaValidationRules(ImmutableList.of(), compatibilityRule);
    }

    public static io.pravega.schemaregistry.contract.data.Compatibility decode(Compatibility compatibility) {
        io.pravega.schemaregistry.contract.data.VersionInfo backwardTill = compatibility.getBackwardTill() == null ? null : decode(compatibility.getBackwardTill());
        io.pravega.schemaregistry.contract.data.VersionInfo forwardTill = compatibility.getForwardTill() == null ? null : decode(compatibility.getForwardTill());
        return new io.pravega.schemaregistry.contract.data.Compatibility(
                searchEnum(io.pravega.schemaregistry.contract.data.Compatibility.Type.class, compatibility.getPolicy().name()),
                backwardTill, forwardTill);
    }

    public static io.pravega.schemaregistry.contract.data.CompressionType decode(CompressionType compressionType) {
        switch (compressionType.getCompressionType()) {
            case CUSTOM:
                return io.pravega.schemaregistry.contract.data.CompressionType.custom(compressionType.getCustomTypeName());
            default:
                return searchEnum(
                        io.pravega.schemaregistry.contract.data.CompressionType.class, compressionType.getCompressionType().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.VersionInfo decode(VersionInfo versionInfo) {
        return new io.pravega.schemaregistry.contract.data.VersionInfo(versionInfo.getSchemaName(), versionInfo.getVersion());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingInfo decode(EncodingInfo encodingInfo) {
        return new io.pravega.schemaregistry.contract.data.EncodingInfo(decode(encodingInfo.getVersionInfo()), 
                decode(encodingInfo.getSchemaInfo()), decode(encodingInfo.getCompressionType()));
    }

    public static <T> io.pravega.schemaregistry.contract.data.SchemaWithVersion decode(SchemaWithVersion schemaWithVersion) {
        return new io.pravega.schemaregistry.contract.data.SchemaWithVersion(decode(schemaWithVersion.getSchemaInfo()),
                decode(schemaWithVersion.getVersion()));
    }

    public static SchemaEvolution decode(SchemaVersionAndRules schemaEvolution) {
        return new io.pravega.schemaregistry.contract.data.SchemaEvolution(decode(schemaEvolution.getSchemaInfo()),
                decode(schemaEvolution.getVersion()), decode(schemaEvolution.getValidationRules()));
    }
    // endregion

    // region encode
    public static SchemaVersionAndRules encode(io.pravega.schemaregistry.contract.data.SchemaEvolution schemaEvolution) {
        SchemaInfo encode = encode(schemaEvolution.getSchema());
        return new SchemaVersionAndRules().schemaInfo(encode)
                                         .version(encode(schemaEvolution.getVersion())).validationRules(encode(schemaEvolution.getRules()));
    }

    public static SchemaValidationRules encode(io.pravega.schemaregistry.contract.data.SchemaValidationRules rules) {
        return new SchemaValidationRules().compatibility(encode(rules.getCompatibility()));
    }

    public static Compatibility encode(io.pravega.schemaregistry.contract.data.Compatibility compatibility) {
        Compatibility policy = new Compatibility().policy(
                searchEnum(Compatibility.PolicyEnum.class, compatibility.getCompatibility().name()));
        if (compatibility.getBackwardTill() != null) {
            VersionInfo backwardTill = encode(compatibility.getBackwardTill());
            policy = policy.backwardTill(backwardTill);
        }
        if (compatibility.getForwardTill() != null) {
            VersionInfo forwardTill = encode(compatibility.getForwardTill());
            policy = policy.forwardTill(forwardTill);
        }
        return policy;
    }

    public static SchemaWithVersion encode(io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion) {
        return new SchemaWithVersion().schemaInfo(encode(schemaWithVersion.getSchema()))
                                           .version(encode(schemaWithVersion.getVersion()));
    }

    public static GroupProperties encode(io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return new GroupProperties()
                .enableEncoding(groupProperties.isEnableEncoding())
                .validateByObjectType(groupProperties.isValidateByObjectType())
                .schemaValidationRules(encode(groupProperties.getSchemaValidationRules()));
    }

    public static GroupProperties encode(String groupName, io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return encode(groupProperties).groupName(groupName);
    }

    public static VersionInfo encode(io.pravega.schemaregistry.contract.data.VersionInfo versionInfo) {
        return new VersionInfo().schemaName(versionInfo.getSchemaName()).version(versionInfo.getVersion());
    }

    public static SchemaInfo encode(io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo) {
        return new SchemaInfo().properties(schemaInfo.getProperties()).schemaData(schemaInfo.getSchemaData())
                                    .schemaName(schemaInfo.getName()).schemaType(encode(schemaInfo.getSchemaType()));
    }

    public static SchemaType encode(io.pravega.schemaregistry.contract.data.SchemaType schemaType) {
        if (schemaType.equals(io.pravega.schemaregistry.contract.data.SchemaType.Custom)) {
            SchemaType schemaTypeModel = new SchemaType().schemaType(SchemaType.SchemaTypeEnum.CUSTOM);
            return schemaTypeModel.customTypeName(schemaType.getCustomTypeName());
        } else {
            return new SchemaType().schemaType(
                    searchEnum(SchemaType.SchemaTypeEnum.class, schemaType.name()));
        }
    }

    public static EncodingId encode(io.pravega.schemaregistry.contract.data.EncodingId encodingId) {
        return new EncodingId().encodingId(encodingId.getId());
    }

    public static CompressionType encode(io.pravega.schemaregistry.contract.data.CompressionType compression) {
        if (compression.equals(io.pravega.schemaregistry.contract.data.CompressionType.Custom)) {
            return new CompressionType().compressionType(CompressionType.CompressionTypeEnum.CUSTOM)
                                             .customTypeName(compression.getCustomTypeName());
        } else {
            return new CompressionType().compressionType(
                    searchEnum(CompressionType.CompressionTypeEnum.class, compression.name()));
        }
    }

    public static EncodingInfo encode(io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo) {
        return new EncodingInfo().compressionType(encode(encodingInfo.getCompression()))
                                      .versionInfo(encode(encodingInfo.getVersionInfo()))
                                      .schemaInfo(encode(encodingInfo.getSchemaInfo()));
    }

    // endregion

    private static <T extends Enum<?>> T searchEnum(Class<T> enumeration,
                                                    String search) {
        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        throw new IllegalArgumentException();
    }
}
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
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolutionEpoch;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.CompatibilityModel;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionTypeModel;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingIdModel;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupPropertiesModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaTypeModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRulesModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersionModel;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfoModel;

public class ModelHelper {

    // region decode
    public static SchemaInfo decode(SchemaInfoModel schemaInfo) {
        SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new SchemaInfo(schemaInfo.getSchemaName(), schemaType, schemaInfo.getSchemaData(),
                ImmutableMap.copyOf(schemaInfo.getProperties()));
    }

    public static SchemaType decode(SchemaTypeModel schemaType) {
        switch (schemaType.getSchemaType()) {
            case CUSTOM:
                return SchemaType.custom(schemaType.getCustomTypeName());
            default:
                return SchemaType.of(SchemaType.Type.valueOf(schemaType.getSchemaType().name()));
        }
    }

    public static SchemaValidationRules decode(SchemaValidationRulesModel rules) {
        Compatibility compatibilityRule = decode(rules.getCompatibility());
        return new SchemaValidationRules(ImmutableList.of(), compatibilityRule);
    }
    
    public static Compatibility decode(CompatibilityModel compatibility) {
        VersionInfo backwardTill = compatibility.getBackwardTill() == null ? null : decode(compatibility.getBackwardTill());
        VersionInfo forwardTill = compatibility.getForwardTill() == null ? null : decode(compatibility.getForwardTill());
        return new Compatibility(
                Compatibility.Type.valueOf(compatibility.getPolicy().name()),
                backwardTill, forwardTill);
    }

    public static CompressionType decode(CompressionTypeModel compressionType) {
        switch (compressionType.getCompressionType()) {
            case CUSTOM:
                return CompressionType.custom(compressionType.getCustomTypeName());
            default:
                return CompressionType.of(CompressionType.Type.valueOf(compressionType.getCompressionType().name()));
        }
    }

    public static VersionInfo decode(VersionInfoModel versionInfo) {
        return new VersionInfo(versionInfo.getSchemaName(), versionInfo.getVersion());
    }
    // endregion

    // region encode
    public static SchemaEvolutionModel encode(SchemaEvolutionEpoch schemaEvolution) {
        SchemaInfoModel encode = encode(schemaEvolution.getSchema());
        return new SchemaEvolutionModel().schemaInfo(encode)
                                         .version(encode(schemaEvolution.getVersion())).validationRules(encode(schemaEvolution.getRules()));
    }

    public static SchemaValidationRulesModel encode(SchemaValidationRules rules) {
        return new SchemaValidationRulesModel().compatibility(encode(rules.getCompatibility()));
    }
    
    public static CompatibilityModel encode(Compatibility compatibility) {
        CompatibilityModel policy = new CompatibilityModel().policy(
                CompatibilityModel.PolicyEnum.fromValue(compatibility.getCompatibility().name()));
        if (compatibility.getBackwardTill() != null) {
            VersionInfoModel backwardTill = encode(compatibility.getBackwardTill());
            policy = policy.backwardTill(backwardTill);
        }
        if (compatibility.getForwardTill() != null) {
            VersionInfoModel forwardTill = encode(compatibility.getForwardTill());
            policy = policy.forwardTill(forwardTill);
        }
        return policy;
    }

    public static SchemaWithVersionModel encode(SchemaWithVersion schemaWithVersion) {
        return new SchemaWithVersionModel().schemaInfo(encode(schemaWithVersion.getSchema()))
                                           .version(encode(schemaWithVersion.getVersion()));
    }

    public static GroupPropertiesModel encode(GroupProperties groupProperties) {
        return new GroupPropertiesModel()
                .enableEncoding(groupProperties.isEnableEncoding())
                .subgroupBySchemaName(groupProperties.isSubgroupBySchemaName())
                .schemaValidationRules(encode(groupProperties.getSchemaValidationRules()));
    }

    public static GroupPropertiesModel encode(String groupName, GroupProperties groupProperties) {
        return ModelHelper.encode(groupProperties).groupName(groupName);
    }

    public static VersionInfoModel encode(VersionInfo versionInfo) {
        return new VersionInfoModel().schemaName(versionInfo.getSchemaName()).version(versionInfo.getVersion());
    }

    public static SchemaInfoModel encode(SchemaInfo schemaInfo) {
        return new SchemaInfoModel().properties(schemaInfo.getProperties()).schemaData(schemaInfo.getSchemaData())
                                    .schemaName(schemaInfo.getName()).schemaType(encode(schemaInfo.getSchemaType()));
    }

    public static SchemaTypeModel encode(SchemaType schemaType) {
        if (schemaType.getSchemaType().equals(SchemaType.Type.Custom)) {
            SchemaTypeModel schemaTypeModel = new SchemaTypeModel().schemaType(SchemaTypeModel.SchemaTypeEnum.CUSTOM);
            return schemaTypeModel.customTypeName(schemaType.getCustomTypeName());
        } else {
            return new SchemaTypeModel().schemaType(SchemaTypeModel.SchemaTypeEnum.fromValue(schemaType.getSchemaType().name()));
        }
    }

    public static EncodingIdModel encode(EncodingId encodingId) {
        return new EncodingIdModel().encodingId(encodingId.getId());
    }
    
    public static CompressionTypeModel encode(CompressionType compression) {
        if (compression.getCompressionType().equals(CompressionType.Type.Custom)) {
            return new CompressionTypeModel().compressionType(CompressionTypeModel.CompressionTypeEnum.CUSTOM)
                                             .customTypeName(compression.getCustomTypeName());
        } else {
            return new CompressionTypeModel().compressionType(CompressionTypeModel.CompressionTypeEnum.fromValue(compression.getCompressionType().name()));
        }
    }
    
    public static EncodingInfoModel encode(EncodingInfo encodingInfo) {
        return new EncodingInfoModel().compressionType(encode(encodingInfo.getCompression()))
                                      .versionInfo(encode(encodingInfo.getVersionInfo()))
                                      .schemaInfo(encode(encodingInfo.getSchemaInfo()));
    }

    // endregion
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRule;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionAndRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides translation (encode/decode) between the Model classes and its REST representation.
 */
public class ModelHelper {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // region decode
    public static io.pravega.schemaregistry.contract.data.SchemaInfo decode(SchemaInfo schemaInfo) {
        io.pravega.schemaregistry.contract.data.SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new io.pravega.schemaregistry.contract.data.SchemaInfo(schemaInfo.getSchemaName(), schemaInfo.getObjectType(), 
                schemaType, schemaInfo.getSchemaData(), ImmutableMap.copyOf(schemaInfo.getProperties()));
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
        List<io.pravega.schemaregistry.contract.data.SchemaValidationRule> list = rules.getRules().entrySet().stream().map(rule -> {
            if (rule.getValue().getRule() instanceof Map) {
                String name = (String) ((Map) rule.getValue().getRule()).get("name");
                if (name.equals(Compatibility.class.getSimpleName())) {
                    return decode(MAPPER.convertValue(rule.getValue().getRule(), Compatibility.class));
                } else {
                    throw new NotImplementedException("Rule not implemented");
                }
            } else if (rule.getValue().getRule() instanceof Compatibility) {
                return decode((Compatibility) rule.getValue().getRule());
            } else {
                throw new IllegalArgumentException("Rule not supported");
            }
        }).collect(Collectors.toList());
        return io.pravega.schemaregistry.contract.data.SchemaValidationRules.of(list);
    }

    public static io.pravega.schemaregistry.contract.data.Compatibility decode(Compatibility compatibility) {
        io.pravega.schemaregistry.contract.data.VersionInfo backwardTill = compatibility.getBackwardTill() == null ? null : decode(compatibility.getBackwardTill());
        io.pravega.schemaregistry.contract.data.VersionInfo forwardTill = compatibility.getForwardTill() == null ? null : decode(compatibility.getForwardTill());
        return new io.pravega.schemaregistry.contract.data.Compatibility(
                searchEnum(io.pravega.schemaregistry.contract.data.Compatibility.Type.class, compatibility.getPolicy().name()),
                backwardTill, forwardTill);
    }

    public static io.pravega.schemaregistry.contract.data.CodecType decode(CodecType codecType) {
        switch (codecType.getCodecType()) {
            case CUSTOM:
                return io.pravega.schemaregistry.contract.data.CodecType.custom(codecType.getCustomTypeName(), codecType.getProperties());
            default:
                return searchEnum(
                        io.pravega.schemaregistry.contract.data.CodecType.class, codecType.getCodecType().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.VersionInfo decode(VersionInfo versionInfo) {
        return new io.pravega.schemaregistry.contract.data.VersionInfo(versionInfo.getSchemaName(), versionInfo.getVersion(), versionInfo.getOrdinal());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingInfo decode(EncodingInfo encodingInfo) {
        return new io.pravega.schemaregistry.contract.data.EncodingInfo(decode(encodingInfo.getVersionInfo()),
                decode(encodingInfo.getSchemaInfo()), decode(encodingInfo.getCodecType()));
    }

    public static io.pravega.schemaregistry.contract.data.SchemaWithVersion decode(SchemaWithVersion schemaWithVersion) {
        return new io.pravega.schemaregistry.contract.data.SchemaWithVersion(decode(schemaWithVersion.getSchemaInfo()),
                decode(schemaWithVersion.getVersion()));
    }

    public static SchemaEvolution decode(SchemaVersionAndRules schemaEvolution) {
        return new io.pravega.schemaregistry.contract.data.SchemaEvolution(decode(schemaEvolution.getSchemaInfo()),
                decode(schemaEvolution.getVersion()), decode(schemaEvolution.getValidationRules()));
    }

    public static io.pravega.schemaregistry.contract.data.EncodingId decode(EncodingId encodingId) {
        return new io.pravega.schemaregistry.contract.data.EncodingId(encodingId.getEncodingId());
    }

    public static io.pravega.schemaregistry.contract.data.GroupProperties decode(GroupProperties groupProperties) {
        return io.pravega.schemaregistry.contract.data.GroupProperties.builder().schemaType(decode(groupProperties.getSchemaType()))
        .schemaValidationRules(decode(groupProperties.getSchemaValidationRules())).validateByObjectType(groupProperties.isValidateByObjectType())
                .properties(groupProperties.getProperties()).build();
    }
    // endregion

    // region encode
    public static SchemaVersionAndRules encode(io.pravega.schemaregistry.contract.data.SchemaEvolution schemaEvolution) {
        SchemaInfo encode = encode(schemaEvolution.getSchema());
        return new SchemaVersionAndRules().schemaInfo(encode)
                                          .version(encode(schemaEvolution.getVersion())).validationRules(encode(schemaEvolution.getRules()));
    }

    public static SchemaValidationRules encode(io.pravega.schemaregistry.contract.data.SchemaValidationRules rules) {
        Map<String, SchemaValidationRule> map = rules.getRules().entrySet().stream().collect(Collectors.toMap(rule -> {
            if (rule.getValue() instanceof io.pravega.schemaregistry.contract.data.Compatibility) {
                return io.pravega.schemaregistry.contract.generated.rest.model.Compatibility.class.getSimpleName();
            } else {
                throw new NotImplementedException("Rule not implemented");
            }
        }, rule -> {
            SchemaValidationRule schemaValidationRule;
            if (rule.getValue() instanceof io.pravega.schemaregistry.contract.data.Compatibility) {
                schemaValidationRule = new SchemaValidationRule().rule(encode((io.pravega.schemaregistry.contract.data.Compatibility) rule.getValue()));
            } else {
                throw new NotImplementedException("Rule not implemented");
            }
            return schemaValidationRule;
        }));
        return new SchemaValidationRules().rules(map);
    }

    public static Compatibility encode(io.pravega.schemaregistry.contract.data.Compatibility compatibility) {
        Compatibility policy = new io.pravega.schemaregistry.contract.generated.rest.model.Compatibility()
                .name(compatibility.getName())
                .policy(searchEnum(Compatibility.PolicyEnum.class, compatibility.getCompatibility().name()));
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
                .schemaType(encode(groupProperties.getSchemaType()))
                .properties(groupProperties.getProperties())
                .validateByObjectType(groupProperties.isValidateByObjectType())
                .schemaValidationRules(encode(groupProperties.getSchemaValidationRules()));
    }

    public static GroupProperties encode(String groupName, io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return encode(groupProperties).groupName(groupName);
    }

    public static VersionInfo encode(io.pravega.schemaregistry.contract.data.VersionInfo versionInfo) {
        return new VersionInfo().schemaName(versionInfo.getObjectType()).version(versionInfo.getVersion());
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

    public static CodecType encode(io.pravega.schemaregistry.contract.data.CodecType codec) {
        if (codec.equals(io.pravega.schemaregistry.contract.data.CodecType.Custom)) {
            return new CodecType().codecType(CodecType.CodecTypeEnum.CUSTOM)
                                  .customTypeName(codec.getCustomTypeName())
                                  .properties(codec.getProperties());
        } else {
            return new CodecType().codecType(
                    searchEnum(CodecType.CodecTypeEnum.class, codec.name()));
        }
    }

    public static EncodingInfo encode(io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo) {
        return new EncodingInfo().codecType(encode(encodingInfo.getCodec()))
                                 .versionInfo(encode(encodingInfo.getVersionInfo()))
                                 .schemaInfo(encode(encodingInfo.getSchemaInfo()));
    }

    // endregion

    private static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search) {
        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        throw new IllegalArgumentException();
    }
}
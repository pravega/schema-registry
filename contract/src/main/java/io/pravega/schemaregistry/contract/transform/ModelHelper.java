/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.generated.rest.model.Backward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardAndForward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardPolicy;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardTill;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardTransitive;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.Forward;
import io.pravega.schemaregistry.contract.generated.rest.model.ForwardPolicy;
import io.pravega.schemaregistry.contract.generated.rest.model.ForwardTill;
import io.pravega.schemaregistry.contract.generated.rest.model.ForwardTransitive;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Provides translation (encode/decode) between the Model classes and its REST representation.
 */
public class ModelHelper {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BACKWARD = Backward.class.getSimpleName();
    private static final String BACKWARD_TRANSITIVE = BackwardTransitive.class.getSimpleName();
    private static final String BACKWARD_TILL = BackwardTill.class.getSimpleName();
    private static final String FORWARD = Forward.class.getSimpleName();
    private static final String FORWARD_TILL = ForwardTill.class.getSimpleName();
    private static final String FORWARD_TRANSITIVE = ForwardTransitive.class.getSimpleName();

    // region decode
    public static io.pravega.schemaregistry.contract.data.SchemaInfo decode(SchemaInfo schemaInfo) {
        Preconditions.checkArgument(schemaInfo != null, "SchemaInfo cannot be null");
        Preconditions.checkArgument(schemaInfo.getType() != null, "SchemaInfo type cannot be null");
        Preconditions.checkArgument(schemaInfo.getSerializationFormat() != null, "Serialization format cannot be null");
        Preconditions.checkArgument(schemaInfo.getSchemaData() != null, "schema data cannot be null");
        io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat = decode(schemaInfo.getSerializationFormat());
        ImmutableMap<String, String> properties = schemaInfo.getProperties() == null ? ImmutableMap.of() : 
                ImmutableMap.copyOf(schemaInfo.getProperties());
        return new io.pravega.schemaregistry.contract.data.SchemaInfo(schemaInfo.getType(),
                serializationFormat, ByteBuffer.wrap(schemaInfo.getSchemaData()), properties);
    }

    public static io.pravega.schemaregistry.contract.data.SerializationFormat decode(SerializationFormat serializationFormat) {
        Preconditions.checkArgument(serializationFormat != null, "serialization format cannot be null");
        switch (serializationFormat.getSerializationFormat()) {
            case CUSTOM:
                Preconditions.checkArgument(serializationFormat.getFullTypeName() != null, "Custom name not supplied");
                return io.pravega.schemaregistry.contract.data.SerializationFormat.custom(serializationFormat.getFullTypeName());
            default:
                return io.pravega.schemaregistry.contract.data.SerializationFormat.withName(
                        searchEnum(io.pravega.schemaregistry.contract.data.SerializationFormat.class, serializationFormat.getSerializationFormat().name()), 
                        serializationFormat.getFullTypeName());
        }
    }

    public static io.pravega.schemaregistry.contract.data.Compatibility decode(Compatibility compatibility) {
        io.pravega.schemaregistry.contract.data.Compatibility decoded;
        switch (compatibility.getPolicy()) {
            case ALLOWANY:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.allowAny();
                break;
            case BACKWARD:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.backward();
                break;
            case BACKWARDTRANSITIVE:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.backwardTransitive();
                break;
            case FORWARD:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.forward();
                break;
            case FORWARDTRANSITIVE:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.forwardTransitive();
                break;
            case FULL:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.full();
                break;
            case FULLTRANSITIVE:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.fullTransitive();
                break;
            case DENYALL:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility.denyAll();
                break;
            case ADVANCED:
                decoded = io.pravega.schemaregistry.contract.data.Compatibility
                        .builder()
                        .type(io.pravega.schemaregistry.contract.data.Compatibility.Type.Advanced)
                        .backwardAndForward(decode(compatibility.getAdvanced())).build();
                break;
            default:
                throw new IllegalArgumentException("Unknown compatibility type");
        }
        return decoded;
    }

    public static io.pravega.schemaregistry.contract.data.BackwardAndForward decode(BackwardAndForward compatibility) {
        Preconditions.checkArgument(compatibility.getBackwardPolicy() != null || compatibility.getForwardPolicy() != null, 
                "At least one of Backward or Forward policy needs to be supplied for Advanced Compatibility");

        io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardAndForwardBuilder builder =
                io.pravega.schemaregistry.contract.data.BackwardAndForward.builder();
        if (compatibility.getBackwardPolicy() != null) {
            builder.backwardPolicy(decode(compatibility.getBackwardPolicy()));
        }
        if (compatibility.getForwardPolicy() != null) {
            builder.forwardPolicy(decode(compatibility.getForwardPolicy()));
        }
        return builder.build();
    }

    public static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardPolicy decode(BackwardPolicy backward) {
        Preconditions.checkArgument(backward != null, "backward policy cannot be null");
        Object obj = backward.getBackwardPolicy();
        if (backward.getBackwardPolicy() instanceof Map) {
            String name = (String) ((Map) backward.getBackwardPolicy()).get("name");
            if (name.equals(BACKWARD)) {
                obj = MAPPER.convertValue(backward.getBackwardPolicy(), Backward.class);
            } else if (name.equals(BACKWARD_TRANSITIVE)) {
                obj = MAPPER.convertValue(backward.getBackwardPolicy(), BackwardTransitive.class);
            } else if (name.equals(BACKWARD_TILL)) {
                obj = MAPPER.convertValue(backward.getBackwardPolicy(), BackwardTill.class);
            } else {
                throw new IllegalArgumentException("Backward policy needs to be one of Backward, BackwardTill or BackwardTransitive");
            }
        }

        if (obj instanceof Backward) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.Backward();
        } else if (obj instanceof BackwardTill) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill(
                    decode(((io.pravega.schemaregistry.contract.generated.rest.model.BackwardTill) backward.getBackwardPolicy()).getVersionInfo()));
        } else if (obj instanceof BackwardTransitive) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTransitive();
        } else {
            throw new IllegalArgumentException("Backward policy needs to be one of Backward, BackwardTill or BackwardTransitive.");
        }
    }

    public static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy decode(io.pravega.schemaregistry.contract.generated.rest.model.ForwardPolicy forward) {
        Preconditions.checkArgument(forward != null, "forward policy cannot be null");

        Object obj = forward.getForwardPolicy();
        if (forward.getForwardPolicy() instanceof Map) {
            String name = (String) ((Map) forward.getForwardPolicy()).get("name");
            if (name.equals(FORWARD)) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), Forward.class);
            } else if (name.equals(FORWARD_TRANSITIVE)) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), ForwardTransitive.class);
            } else if (name.equals(FORWARD_TILL)) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), ForwardTill.class);
            } else {
                throw new IllegalArgumentException("Forward policy needs to be one of Forward, ForwardTill or ForwardTransitive.");
            }
        }

        if (obj instanceof Forward) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.Forward();
        } else if (obj instanceof ForwardTill) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill(
                    decode(((io.pravega.schemaregistry.contract.generated.rest.model.ForwardTill) forward.getForwardPolicy()).getVersionInfo()));
        } else if (obj instanceof ForwardTransitive) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive();
        } else {
            throw new IllegalArgumentException("Forward policy needs to be one of Forward, ForwardTill or ForwardTransitive.");
        }
    }

    public static io.pravega.schemaregistry.contract.data.VersionInfo decode(VersionInfo versionInfo) {
        Preconditions.checkArgument(versionInfo != null, "Version info cannot be null");
        Preconditions.checkArgument(versionInfo.getType() != null, "type cannot be null");
        Preconditions.checkArgument(versionInfo.getVersion() != null, "version cannot be null");
        Preconditions.checkArgument(versionInfo.getId() != null, "id cannot be null");
        return new io.pravega.schemaregistry.contract.data.VersionInfo(versionInfo.getType(), versionInfo.getVersion(), versionInfo.getId());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingInfo decode(EncodingInfo encodingInfo) {
        Preconditions.checkArgument(encodingInfo != null, "EncodingInfo cannot be null");
        Preconditions.checkArgument(encodingInfo.getVersionInfo() != null, "VersionInfo cannot be null");
        Preconditions.checkArgument(encodingInfo.getSchemaInfo() != null, "SchemaInfo cannot be null");
        Preconditions.checkArgument(encodingInfo.getCodecType() != null, "CodecType cannot be null");
        return new io.pravega.schemaregistry.contract.data.EncodingInfo(decode(encodingInfo.getVersionInfo()),
                decode(encodingInfo.getSchemaInfo()), decode(encodingInfo.getCodecType()));
    }

    public static io.pravega.schemaregistry.contract.data.CodecType decode(CodecType codecType) {
        Preconditions.checkArgument(codecType != null, "CodecType cannot be null");
        Preconditions.checkArgument(codecType.getName() != null, "CodecType.name cannot be null");
        return codecType.getProperties() == null ? new io.pravega.schemaregistry.contract.data.CodecType(codecType.getName()) :
                new io.pravega.schemaregistry.contract.data.CodecType(codecType.getName(), ImmutableMap.copyOf(codecType.getProperties()));
    }

    public static io.pravega.schemaregistry.contract.data.SchemaWithVersion decode(SchemaWithVersion schemaWithVersion) {
        Preconditions.checkArgument(schemaWithVersion != null, "schema with version cannot be null");
        Preconditions.checkArgument(schemaWithVersion.getVersionInfo() != null, "VersionInfo cannot be null");
        Preconditions.checkArgument(schemaWithVersion.getSchemaInfo() != null, "SchemaInfo cannot be null");
        return new io.pravega.schemaregistry.contract.data.SchemaWithVersion(decode(schemaWithVersion.getSchemaInfo()),
                decode(schemaWithVersion.getVersionInfo()));
    }

    public static io.pravega.schemaregistry.contract.data.GroupHistoryRecord decode(GroupHistoryRecord historyRecord) {
        Preconditions.checkArgument(historyRecord != null, "history record be null");
        Preconditions.checkArgument(historyRecord.getSchemaInfo() != null, "schemaInfo be null");
        Preconditions.checkArgument(historyRecord.getVersionInfo() != null, "versionInfo be null");
        Preconditions.checkArgument(historyRecord.getTimestamp() != null, "Timestamp be null");
        Preconditions.checkArgument(historyRecord.getCompatibility() != null, "Compatibility be null");

        return new io.pravega.schemaregistry.contract.data.GroupHistoryRecord(decode(historyRecord.getSchemaInfo()),
                decode(historyRecord.getVersionInfo()), decode(historyRecord.getCompatibility()), historyRecord.getTimestamp(),
                historyRecord.getSchemaString());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingId decode(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null, "EncodingId cannot be null");
        Preconditions.checkArgument(encodingId.getEncodingId() != null, "EncodingId cannot be null");
        Preconditions.checkArgument(encodingId.getEncodingId() != null);

        return new io.pravega.schemaregistry.contract.data.EncodingId(encodingId.getEncodingId());
    }

    public static io.pravega.schemaregistry.contract.data.GroupProperties decode(GroupProperties groupProperties) {
        Preconditions.checkArgument(groupProperties != null, "group properties cannot be null");
        Preconditions.checkArgument(groupProperties.isAllowMultipleTypes() != null, "is allow multiple type cannot be null");
        Preconditions.checkArgument(groupProperties.getSerializationFormat() != null, "serialization format cannot be null");
        Preconditions.checkArgument(groupProperties.getCompatibility() != null, "compatibility cannot be null");

        ImmutableMap<String, String> properties = groupProperties.getProperties() == null ? ImmutableMap.of() : 
                ImmutableMap.copyOf(groupProperties.getProperties());
        return io.pravega.schemaregistry.contract.data.GroupProperties.builder().serializationFormat(decode(groupProperties.getSerializationFormat()))
                                                                      .compatibility(decode(groupProperties.getCompatibility())).allowMultipleTypes(groupProperties.isAllowMultipleTypes())
                                                                      .properties(properties).build();
    }
    // endregion

    // region encode
    public static GroupHistoryRecord encode(io.pravega.schemaregistry.contract.data.GroupHistoryRecord groupHistoryRecord) {
        return new GroupHistoryRecord().schemaInfo(encode(groupHistoryRecord.getSchemaInfo()))
                                       .versionInfo(encode(groupHistoryRecord.getVersionInfo()))
                                       .compatibility(encode(groupHistoryRecord.getCompatibility()))
                                       .timestamp(groupHistoryRecord.getTimestamp())
                                       .schemaString(groupHistoryRecord.getSchemaString());
    }

    public static Compatibility encode(io.pravega.schemaregistry.contract.data.Compatibility compatibility) {
        Compatibility policy = new io.pravega.schemaregistry.contract.generated.rest.model.Compatibility()
                .policy(searchEnum(Compatibility.PolicyEnum.class, compatibility.getType().name()));
        if (policy.getPolicy().equals(Compatibility.PolicyEnum.ADVANCED)) {
            policy.advanced(encode(compatibility.getBackwardAndForward()));
        }
        return policy;
    }

    public static BackwardAndForward encode(io.pravega.schemaregistry.contract.data.BackwardAndForward backwardAndForward) {
        BackwardAndForward retVal = new BackwardAndForward();
        if (backwardAndForward.getBackwardPolicy() != null) {
            retVal.backwardPolicy(encode(backwardAndForward.getBackwardPolicy()));
        }
        if (backwardAndForward.getForwardPolicy() != null) {
            retVal.forwardPolicy(encode(backwardAndForward.getForwardPolicy()));
        }
        return retVal;
    }

    public static BackwardPolicy encode(io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardPolicy backwardPolicy) {
        if (backwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.Backward) {
            return new BackwardPolicy().backwardPolicy(new Backward().name(Backward.class.getSimpleName()));
        } else if (backwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTransitive) {
            return new BackwardPolicy().backwardPolicy(new BackwardTransitive().name(BackwardTransitive.class.getSimpleName()));
        } else if (backwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill) {
            VersionInfo version = encode(((io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill) backwardPolicy).getVersionInfo());
            return new BackwardPolicy().backwardPolicy(new BackwardTill().name(BackwardTill.class.getSimpleName()).versionInfo(version));
        } else {
            throw new IllegalArgumentException("Backward policy needs to be one of Backward BackwardTill or BackwardTransitive");
        }
    }

    public static ForwardPolicy encode(io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy forwardPolicy) {
        if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.Forward) {
            return new ForwardPolicy().forwardPolicy(new Forward().name(Forward.class.getSimpleName()));
        } else if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive) {
            return new ForwardPolicy().forwardPolicy(new ForwardTransitive().name(ForwardTransitive.class.getSimpleName()));
        } else if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill) {
            VersionInfo version = encode(((io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill) forwardPolicy).getVersionInfo());
            return new ForwardPolicy().forwardPolicy(new ForwardTill().name(ForwardTill.class.getSimpleName()).versionInfo(version));
        } else {
            throw new IllegalArgumentException("Forward policy needs to be one of Forward ForwardTill or ForwardTransitive");
        }
    }

    public static SchemaWithVersion encode(io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion) {
        return new SchemaWithVersion().schemaInfo(encode(schemaWithVersion.getSchemaInfo()))
                                      .versionInfo(encode(schemaWithVersion.getVersionInfo()));
    }

    public static GroupProperties encode(io.pravega.schemaregistry.contract.data.GroupProperties groupProperties) {
        return new GroupProperties()
                .serializationFormat(encode(groupProperties.getSerializationFormat()))
                .properties(groupProperties.getProperties())
                .allowMultipleTypes(groupProperties.isAllowMultipleTypes())
                .compatibility(encode(groupProperties.getCompatibility()));
    }

    public static VersionInfo encode(io.pravega.schemaregistry.contract.data.VersionInfo versionInfo) {
        return new VersionInfo().type(versionInfo.getType()).version(versionInfo.getVersion()).id(versionInfo.getId());
    }

    public static SchemaInfo encode(io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo) {
        return new SchemaInfo().properties(schemaInfo.getProperties()).schemaData(schemaInfo.getSchemaData().array())
                               .type(schemaInfo.getType()).serializationFormat(encode(schemaInfo.getSerializationFormat()));
    }

    public static SerializationFormat encode(io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat) {
        if (serializationFormat.equals(io.pravega.schemaregistry.contract.data.SerializationFormat.Custom)) {
            Preconditions.checkArgument(serializationFormat.getFullTypeName() != null);
            SerializationFormat serializationFormatModel = new SerializationFormat().serializationFormat(SerializationFormat.SerializationFormatEnum.CUSTOM);
            return serializationFormatModel.fullTypeName(serializationFormat.getFullTypeName());
        } else {
            return new SerializationFormat().serializationFormat(
                    searchEnum(SerializationFormat.SerializationFormatEnum.class, serializationFormat.name()))
                                            .fullTypeName(serializationFormat.getFullTypeName());
        }
    }

    public static EncodingId encode(io.pravega.schemaregistry.contract.data.EncodingId encodingId) {
        return new EncodingId().encodingId(encodingId.getId());
    }

    public static EncodingInfo encode(io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo) {
        return new EncodingInfo().codecType(encode(encodingInfo.getCodecType()))
                                 .versionInfo(encode(encodingInfo.getVersionInfo()))
                                 .schemaInfo(encode(encodingInfo.getSchemaInfo()));
    }

    public static CodecType encode(io.pravega.schemaregistry.contract.data.CodecType codecType) {
        return new CodecType().name(codecType.getName())
                              .properties(codecType.getProperties());
    }

    // endregion

    private static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search) {
        for (T each : enumeration.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        throw new IllegalArgumentException(String.format("Value %s not found in enum %s", search, enumeration.getSimpleName()));
    }
}
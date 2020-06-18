/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.generated.rest.model.AllowAny;
import io.pravega.schemaregistry.contract.generated.rest.model.Backward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardAndForward;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardPolicy;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardTill;
import io.pravega.schemaregistry.contract.generated.rest.model.BackwardTransitive;
import io.pravega.schemaregistry.contract.generated.rest.model.Compatibility;
import io.pravega.schemaregistry.contract.generated.rest.model.DenyAll;
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
    private static final String ALLOW_ANY = AllowAny.class.getSimpleName();
    private static final String DENY_ALL = DenyAll.class.getSimpleName();
    private static final String BACKWARD_AND_FORWARD = BackwardAndForward.class.getSimpleName();
    private static final String BACKWARD_TRANSITIVE = BackwardTransitive.class.getSimpleName();
    private static final String BACKWARD_TILL = BackwardTill.class.getSimpleName();
    private static final String FORWARD = Forward.class.getSimpleName();
    private static final String FORWARD_TILL = ForwardTill.class.getSimpleName();

    // region decode
    public static io.pravega.schemaregistry.contract.data.SchemaInfo decode(SchemaInfo schemaInfo) {
        Preconditions.checkArgument(schemaInfo != null);
        Preconditions.checkArgument(schemaInfo.getType() != null);
        Preconditions.checkArgument(schemaInfo.getSerializationFormat() != null);
        Preconditions.checkArgument(schemaInfo.getProperties() != null);
        Preconditions.checkArgument(schemaInfo.getSchemaData() != null);
        io.pravega.schemaregistry.contract.data.SerializationFormat serializationFormat = decode(schemaInfo.getSerializationFormat());
        return new io.pravega.schemaregistry.contract.data.SchemaInfo(schemaInfo.getType(),
                serializationFormat, ByteBuffer.wrap(schemaInfo.getSchemaData()), ImmutableMap.copyOf(schemaInfo.getProperties()));
    }

    public static io.pravega.schemaregistry.contract.data.SerializationFormat decode(SerializationFormat serializationFormat) {
        Preconditions.checkArgument(serializationFormat != null);
        switch (serializationFormat.getSerializationFormat()) {
            case CUSTOM:
                Preconditions.checkArgument(serializationFormat.getCustomTypeName() != null);
                return io.pravega.schemaregistry.contract.data.SerializationFormat.custom(serializationFormat.getCustomTypeName());
            default:
                return searchEnum(io.pravega.schemaregistry.contract.data.SerializationFormat.class, serializationFormat.getSerializationFormat().name());
        }
    }

    public static io.pravega.schemaregistry.contract.data.Compatibility decode(Compatibility compatibility) {
        io.pravega.schemaregistry.contract.data.Compatibility.Type type = searchEnum(io.pravega.schemaregistry.contract.data.Compatibility.Type.class, compatibility.getPolicy().name());
        switch (type) {
            case AllowAny:
                return io.pravega.schemaregistry.contract.data.Compatibility.allowAny();
            case DenyAll:
                return io.pravega.schemaregistry.contract.data.Compatibility.denyAll();
            case BackwardAndForward:
                return io.pravega.schemaregistry.contract.data.Compatibility
                        .builder()
                        .type(type)
                        .backwardAndForward(decode(compatibility.getBackwardAndForward())).build();
            default:
                throw new IllegalArgumentException();
        }
    }

    public static io.pravega.schemaregistry.contract.data.BackwardAndForward decode(BackwardAndForward compatibility) {
        Preconditions.checkArgument(compatibility.getBackwardPolicy() != null || compatibility.getForwardPolicy() != null);

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
                throw new IllegalArgumentException();
            }
        }

        if (obj instanceof Backward) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.Backward();
        } else if (obj instanceof BackwardTill) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill(
                    decode(((io.pravega.schemaregistry.contract.generated.rest.model.BackwardTill) backward.getBackwardPolicy()).getVersion()));
        } else if (obj instanceof BackwardTransitive) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTransitive();
        } else {
            throw new IllegalArgumentException("Rule not supported");
        }
    }

    public static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy decode(io.pravega.schemaregistry.contract.generated.rest.model.ForwardPolicy forward) {
        Object obj = forward.getForwardPolicy();
        if (forward.getForwardPolicy() instanceof Map) {
            String name = (String) ((Map) forward.getForwardPolicy()).get("name");
            if (name.equals(FORWARD)) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), Forward.class);
            } else if (name.equals(ForwardTransitive.class.getSimpleName())) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), ForwardTransitive.class);
            } else if (name.equals(FORWARD_TILL)) {
                obj = MAPPER.convertValue(forward.getForwardPolicy(), ForwardTill.class);
            } else {
                throw new IllegalArgumentException();
            }
        }

        if (obj instanceof Forward) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.Forward();
        } else if (obj instanceof ForwardTill) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill(
                    decode(((io.pravega.schemaregistry.contract.generated.rest.model.ForwardTill) forward.getForwardPolicy()).getVersion()));
        } else if (obj instanceof ForwardTransitive) {
            return new io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive();
        } else {
            throw new IllegalArgumentException("Rule not supported");
        }
    }

    public static io.pravega.schemaregistry.contract.data.VersionInfo decode(VersionInfo versionInfo) {
        Preconditions.checkArgument(versionInfo != null);
        Preconditions.checkArgument(versionInfo.getType() != null);
        Preconditions.checkArgument(versionInfo.getVersion() != null);
        Preconditions.checkArgument(versionInfo.getId() != null);
        return new io.pravega.schemaregistry.contract.data.VersionInfo(versionInfo.getType(), versionInfo.getVersion(), versionInfo.getId());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingInfo decode(EncodingInfo encodingInfo) {
        Preconditions.checkArgument(encodingInfo != null);
        return new io.pravega.schemaregistry.contract.data.EncodingInfo(decode(encodingInfo.getVersionInfo()),
                decode(encodingInfo.getSchemaInfo()), encodingInfo.getCodecType());
    }

    public static io.pravega.schemaregistry.contract.data.SchemaWithVersion decode(SchemaWithVersion schemaWithVersion) {
        Preconditions.checkArgument(schemaWithVersion != null);
        return new io.pravega.schemaregistry.contract.data.SchemaWithVersion(decode(schemaWithVersion.getSchemaInfo()),
                decode(schemaWithVersion.getVersion()));
    }

    public static io.pravega.schemaregistry.contract.data.GroupHistoryRecord decode(GroupHistoryRecord schemaEvolution) {
        Preconditions.checkArgument(schemaEvolution != null);

        return new io.pravega.schemaregistry.contract.data.GroupHistoryRecord(decode(schemaEvolution.getSchemaInfo()),
                decode(schemaEvolution.getVersion()), decode(schemaEvolution.getCompatibility()), schemaEvolution.getTimestamp(),
                schemaEvolution.getSchemaString());
    }

    public static io.pravega.schemaregistry.contract.data.EncodingId decode(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null);
        Preconditions.checkArgument(encodingId.getEncodingId() != null);

        return new io.pravega.schemaregistry.contract.data.EncodingId(encodingId.getEncodingId());
    }

    public static io.pravega.schemaregistry.contract.data.GroupProperties decode(GroupProperties groupProperties) {
        Preconditions.checkArgument(groupProperties != null);
        Preconditions.checkArgument(groupProperties.isAllowMultipleTypes() != null);

        return io.pravega.schemaregistry.contract.data.GroupProperties.builder().serializationFormat(decode(groupProperties.getSerializationFormat()))
                                                                      .compatibility(decode(groupProperties.getCompatibility())).allowMultipleTypes(groupProperties.isAllowMultipleTypes())
                                                                      .properties(ImmutableMap.copyOf(groupProperties.getProperties())).build();
    }
    // endregion

    // region encode
    public static GroupHistoryRecord encode(io.pravega.schemaregistry.contract.data.GroupHistoryRecord groupHistoryRecord) {
        return new GroupHistoryRecord().schemaInfo(encode(groupHistoryRecord.getSchema()))
                                       .version(encode(groupHistoryRecord.getVersion()))
                                       .compatibility(encode(groupHistoryRecord.getCompatibility()))
                                       .timestamp(groupHistoryRecord.getTimestamp())
                                       .schemaString(groupHistoryRecord.getSchemaString());
    }

    public static Compatibility encode(io.pravega.schemaregistry.contract.data.Compatibility compatibility) {
        Compatibility policy = new io.pravega.schemaregistry.contract.generated.rest.model.Compatibility()
                .policy(searchEnum(Compatibility.PolicyEnum.class, compatibility.getType().name()));
        if (policy.getPolicy().equals(Compatibility.PolicyEnum.BACKWARDANDFORWARD)) {
            policy.backwardAndForward(encode(compatibility.getBackwardAndForward()));
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
            return new BackwardPolicy().backwardPolicy(new BackwardTill().name(BackwardTill.class.getSimpleName()).version(version));
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static ForwardPolicy encode(io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy forwardPolicy) {
        if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.Forward) {
            return new ForwardPolicy().forwardPolicy(new Forward().name(Forward.class.getSimpleName()));
        } else if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive) {
            return new ForwardPolicy().forwardPolicy(new ForwardTransitive().name(ForwardTransitive.class.getSimpleName()));
        } else if (forwardPolicy instanceof io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill) {
            VersionInfo version = encode(((io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill) forwardPolicy).getVersionInfo());
            return new ForwardPolicy().forwardPolicy(new ForwardTill().name(ForwardTill.class.getSimpleName()).version(version));
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static SchemaWithVersion encode(io.pravega.schemaregistry.contract.data.SchemaWithVersion schemaWithVersion) {
        return new SchemaWithVersion().schemaInfo(encode(schemaWithVersion.getSchemaInfo()))
                                      .version(encode(schemaWithVersion.getVersionInfo()));
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
            Preconditions.checkArgument(serializationFormat.getCustomTypeName() != null);
            SerializationFormat serializationFormatModel = new SerializationFormat().serializationFormat(SerializationFormat.SerializationFormatEnum.CUSTOM);
            return serializationFormatModel.customTypeName(serializationFormat.getCustomTypeName());
        } else {
            return new SerializationFormat().serializationFormat(
                    searchEnum(SerializationFormat.SerializationFormatEnum.class, serializationFormat.name()));
        }
    }

    public static EncodingId encode(io.pravega.schemaregistry.contract.data.EncodingId encodingId) {
        return new EncodingId().encodingId(encodingId.getId());
    }

    public static EncodingInfo encode(io.pravega.schemaregistry.contract.data.EncodingInfo encodingInfo) {
        return new EncodingInfo().codecType(encodingInfo.getCodecType())
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
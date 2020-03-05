package io.pravega.schemaregistry;

import lombok.Getter;
import lombok.Setter;

public enum SchemaFormat {
    None,
    Avro,
    Protobuf,
    Json,
    Custom;
    
    @Setter
    @Getter
    private String customName;
    
    public static SchemaFormat custom(String customName) {
        SchemaFormat custom = SchemaFormat.Custom;
        custom.setCustomName(customName);
        return custom;
    }
}

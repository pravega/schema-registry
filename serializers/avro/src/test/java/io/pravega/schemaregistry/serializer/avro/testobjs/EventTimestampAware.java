package io.pravega.schemaregistry.serializer.avro.testobjs;

public interface EventTimestampAware {
    void setEventTimestamp(Long value);

    Long getEventTimestamp();
}

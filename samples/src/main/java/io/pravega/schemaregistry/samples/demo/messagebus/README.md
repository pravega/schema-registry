<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
A message queue in which multiple types of messages. It has two implementations - 1. protobuf 2. avro. 
The schema is registered with the registry service and an encoding header is included with each event that is serialized and 
written into pravega stream. 

This package has three different consumers -
1. Typed Consumer which reads typed events back from the stream using the generated objects. 
This uses a deserializer that reads the header to get the schema but only uses the type from it to identify the object type.
It then uses the respective `protobuf/avro` generated java class to deserialize the data into the read time schema.

2. Typed Consumer with fallback to generic deserialization.
This uses a deserializer that is similar to `1` for all the known types. For any event for which a deserializer is not provided, 
it uses the writer schema from the registry to deserialize the event into `protobuf.DynamicMessage/avro.GenericRecord` object. 

3. Generic consumer.
This uses the generic `protobuf/avro` deserializer to deserialize all events into `protobuf.DynamicMessage/avro.GenericRecord`.    
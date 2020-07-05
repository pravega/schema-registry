<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
A toy sql app which supports create table and select * from table. 
The table is created against the data in a pravega stream by retrieving the latest schema for the group corresponding to the stream. 

This package has three different writers that write data into the stream with three different schemas. 
The group is created with backwardPolicy Compatibility.
There are three avro schemas in this package -
1. schema1: has a record of type User which has two fields name and age.
2. schema2: adds one more field to User's schema1 definition. This field is called address and has a default value. 
This makes the schema both backwardPolicy and forwardPolicy compatible with schema1. 
3. schema3: adds another field called social security which does not have a default value. 
This makes schema 3 backwardPolicy incompatible with schema 2. Which means schema3 cannot be used to read data written using schema2 or schema1.

We can start the sql app and run following commands:
1. Create Table:
```create table <table-name> from pravega:<scope>/<stream>```

This will create a new table with the given table name against the data in supplied pravega stream.
It fetches the latest schema for the group corresponding to the stream from the registry service and uses the fields there
for table's schema.
It first creates a new group in the registry for storing table schema. 
Then it generates a new custom schema format called and names it "tableSchema" and registers this schema with the schema registry
in the new table specific group. 

2. Select:
```Select * from <table-name>```
This retrieves the "tableSchema" from the registry. It then reads all records in the stream using avro generic deserializer. 
And it populates the table query results by applying the table schema on the result. 

This sample highlights how schema registry enables building applications like sql.
It also demonstrates how schema storage is not limited to pravega streams and groups can be created for any custom usage and 
schemas stored against it (e.g. table schema). 
It also demonstrates that schema format in the registry is not limited to json avro or protobuf and can be arbitrary custom format too.
Schema registry service only performs compatibility checks for avro, and for all other formats either AllowAny or DenyAll are the 
acceptable compatibility policies. 

This sample can also be used to highlight that after registering schema1 and schema2 if schema3 is attempted to be registered, 
the registry service will reject it because it breaks the compatibility policy for the group. 

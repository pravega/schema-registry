# REST API Usage
<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

## REST API Sample curl commands
Schema Registry Service provides a RESTful interface for storing and managing schemas under schema groups. Users create a named schema group under which they store and evolve the schemas according to desired compatibility policy. 

REST API documentation could be found [here](rest-documentation.md).

Following are example usage of REST apis in schema registry:

#### Create a new Group
```
$ cat groupProperties.json
{
   "groupName": "mygroup",
   "groupProperties": {
      "serializationFormat":{
         "serializationFormat":"Avro"
      },
      "compatibility":{
         "policy":"BackwardTransitive"
      },
      "allowMultipleTypes":true,
      "properties":{ }
}

$ curl -X  POST http://SchemaRegistryIP:Port/v1/groups -H "accept: application/json" -H "Content-Type: application/json" -d @groupProperties.json
```

#### List all groups
```
$ curl -X GET http://SchemaRegistryIP:Port/v1/groups?limit={limit}&continuationToken={continuationToken} 
{
  "groups": {
    "mygroup": {
      "serializationFormat": {
        "serializationFormat": "Avro"
      },
      "compatibility": {
        "policy": "BackwardTransitive"
      },
      "allowMultipleTypes": true,
      "properties": {}
    }
  },
  "continuationToken": "1"
}
```
#### Add a new version of a schema under the group. 
Schema binary is base64 encoded and wrapped in a schema info object
```
$ cat User.json
{
  "type": "record",
  "name": "User",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    }
  ]
}
$ cat User.json | base64
eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IlVzZXIiLCJmaWVsZHMiOlt7Im5hbWUiOiJuYW1lIiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6ImFnZSIsInR5cGUiOiJpbnQifV19Cg==

$ curl -X POST "http://SchemaRegistryOP:Port/v1/groups/mygroup/schemas" -H "accept: application/json" -H "Content-Type: application/json" -d "{ \"type\": \"User\", \"serializationFormat\": { \"serializationFormat\": \"Avro\" }, \"schemaData\": \"eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IlVzZXIiLCJmaWVsZHMiOlt7Im5hbWUiOiJuYW1lIiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6ImFnZSIsInR5cGUiOiJpbnQifV19Cg==\", \"properties\": { }}"

{
  "type": "User",
  "version": 0,
  "id": 0
}
```

Add schema of different type
```
$ cat Address.json
{
  "type": "record",
  "name": "Address",
  "fields": [
    {
      "name": "street-address",
      "type": "string"
    },
    {
      "name": "zip",
      "type": "int"
    }
  ]
}
$ cat Address.json | base64
eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IkFkZHJlc3MiLCJmaWVsZHMiOlt7Im5hbWUiOiJzdHJlZXQtYWRkcmVzcyIsInR5cGUiOiJzdHJpbmcifSx7Im5hbWUiOiJ6aXAiLCJ0eXBlIjoiaW50In1dfQo=

$ curl -X POST "http://localhost:9092/v1/groups/group/schemas" -H "accept: application/json" -H "Content-Type: application/json" -d "{ \"type\": \"Address\", \"serializationFormat\": { \"serializationFormat\": \"Avro\" }, \"schemaData\": \"eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IkFkZHJlc3MiLCJmaWVsZHMiOlt7Im5hbWUiOiJhZGRyZXNzIiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6InppcCIsInR5cGUiOiJpbnQifV19Cg==\", \"properties\": { }}"
{"type":"Address","version":0,"id":1}
```

#### List latest schemas for type User 
Without type query parameter the api would return latest schemas for all schema types.
```
$ curl -X GET  http://SchemaRegistryIP:Port/v1/groups/mygroup/schemas?type=User
{
  "schemas": [
    {
      "schemaInfo": {
        "type": "User",
        "serializationFormat": {
          "serializationFormat": "Avro"
        },
        "schemaData": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IlVzZXIiLCJuYW1lc3BhY2UiOiJpby5wcmF2ZWdhIiwiZmllbGRzIjpbeyJuYW1lIjoibmFtZSIsInR5cGUiOiJzdHJpbmcifSx7Im5hbWUiOiJhZ2UiLCJ0eXBlIjoiaW50In1dfQ==",
        "properties": {}
      },
      "versionInfo": {
        "type": "User",
        "version": 0,
        "id": 0
      }
    }
  ]
}
```

#### List all schema versions for type User added under the group
```
$ curl -X GET  http://SchemaRegistryIP:Port/v1/groups/mygroup/schemas/versions?type=User 
{
  "schemas": [
    {
      "schemaInfo": {
        "type": "User",
        "serializationFormat": {
          "serializationFormat": "Avro"
        },
        "schemaData": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6IlVzZXIiLCJmaWVsZHMiOlt7Im5hbWUiOiJuYW1lIiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6ImFnZSIsInR5cGUiOiJpbnQifV19",
        "properties": {}
      },
      "versionInfo": {
        "type": "User",
        "version": 0,
        "id": 0
      }
    }
  ]
}
```
#### Delete schema by schema id 
```
$ curl -X DELETE http://SchemaRegistryIP:Port/v1/groups/mygroup/schemas/schema/0
```

#### Delete schema group 
```
$ curl -X DELETE http://SchemaRegistryIP:Port/v1/groups/mygroup
```


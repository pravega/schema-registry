# REST API Documentation
<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

## Version: 0.0.1

**License:** [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

### /groups

#### GET
##### Description:

List all groups within the namespace. If namespace is not specified, All groups in default namespace are listed.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| namespace | query | Namespace in which to look up groups | No | string |
| continuationToken | query | Continuation token | No | string |
| limit | query | The numbers of items to return | No | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | List of all groups | [ListGroupsResponse](#listgroupsresponse) |
| 500 | Internal server error while fetching the list of Groups |  |

#### POST
##### Description:

Create a new Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| namespace | query | Namespace in which to create group | No | string |
| CreateGroupRequest | body | The Group configuration | Yes | object |

##### Responses

| Code | Description |
| ---- | ----------- |
| 201 | Successfully added group |
| 409 | Group with given name already exists |
| 500 | Internal server error while creating a Group |

### /groups/{groupName}

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Group properties | [GroupProperties](#groupproperties) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

#### DELETE
##### Description:

Delete a Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 204 | Successfully deleted the Group |
| 500 | Internal server error while deleting the Group |

### /groups/{groupName}/history

#### GET
##### Description:

Fetch the history of schema evolution of a Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Group history | [GroupHistory](#grouphistory) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group history |  |

### /groups/{groupName}/compatibility

#### PUT
##### Description:

update schema compatibility of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| UpdateCompatibilityRequest | body | update group policy | Yes | object |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 | Updated schema compatibility policy |
| 404 | Group with given name not found |
| 409 | Write conflict |
| 500 | Internal server error while updating Group's schema compatibility |

### /groups/{groupName}/schemas

#### GET
##### Description:

Fetch latest schema versions for all objects identified by SchemaInfo#type under a Group. If query param type is specified then latest schema for the type is returned.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| type | query | Type of object | No | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Latest schemas for all objects identified by SchemaInfo#type under the group | [SchemaVersionsList](#schemaversionslist) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group's latest schemas |  |

#### POST
##### Description:

Adds a new schema to the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| type | query | Type of object | No | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| schemaInfo | body | Add new schema to group | Yes | [SchemaInfo](#schemainfo) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 201 | Successfully added schema to the group | [VersionInfo](#versioninfo) |
| 404 | Group not found |  |
| 409 | Incompatible schema |  |
| 417 | Invalid serialization format |  |
| 500 | Internal server error while adding schema to group |  |

### /groups/{groupName}/schemas/versions

#### GET
##### Description:

Get all schema versions for the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| type | query | Type of object the schema describes. | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Versioned history of schemas registered under the group | [SchemaVersionsList](#schemaversionslist) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group schema versions |  |

### /groups/{groupName}/schemas/versions/find

#### POST
##### Description:

Get the version for the schema if it is registered. It does not automatically register the schema. To add new schema use addSchema

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| schemaInfo | body | Get schema corresponding to the version | Yes | [SchemaInfo](#schemainfo) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema version | [VersionInfo](#versioninfo) |
| 404 | Group with given name not found |  |
| 500 | Internal server error fetching version for schema |  |

### /groups/{groupName}/schemas/schema/{schemaId}

#### GET
##### Description:

Get schema from the schema id that uniquely identifies the schema in the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| schemaId | path | Schema Id | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema corresponding to the version | [SchemaInfo](#schemainfo) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching schema from version |  |

#### DELETE
##### Description:

Delete schema identified by version from the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| schemaId | path | Schema Id | Yes | integer |

##### Responses

| Code | Description |
| ---- | ----------- |
| 204 | Schema corresponding to the version |
| 404 | Group with given name not found |
| 500 | Internal server error while deleting schema from group |

### /groups/{groupName}/schemas/{type}/versions/{version}

#### GET
##### Description:

Get schema from the version number that uniquely identifies the schema in the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| type | path | Schema type from SchemaInfo#type or VersionInfo#type | Yes | string |
| version | path | Version number | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema corresponding to the version | [SchemaInfo](#schemainfo) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching schema from version |  |

#### DELETE
##### Description:

Delete schema version from the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| type | path | Schema type from SchemaInfo#type or VersionInfo#type | Yes | string |
| version | path | Version number | Yes | integer |

##### Responses

| Code | Description |
| ---- | ----------- |
| 204 | Schema corresponding to the version |
| 404 | Group with given name not found |
| 500 | Internal server error while deleting schema from group |

### /groups/{groupName}/schemas/versions/validate

#### POST
##### Description:

Checks if given schema is compatible with schemas in the registry for current policy setting.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| ValidateRequest | body | Checks if schema is valid with respect to supplied compatibility | Yes | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema validation response | [Valid](#valid) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while trying to validate schema |  |

### /groups/{groupName}/schemas/versions/canRead

#### POST
##### Description:

Checks if given schema can be used for reads subject to compatibility policy in the schema compatibility.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| schemaInfo | body | Checks if schema can be used to read the data in the stream based on compatibility policy. | Yes | [SchemaInfo](#schemainfo) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Response to tell whether schema can be used to read existing schemas | [CanRead](#canread) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while checking schema for readability |  |

### /groups/{groupName}/encodings

#### PUT
##### Description:

Get an encoding id that uniquely identifies a schema version and codec type pair.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| GetEncodingIdRequest | body | Get schema corresponding to the version | Yes | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Encoding | [EncodingId](#encodingid) |
| 404 | Group with given name or version not found |  |
| 412 | Codec type not registered |  |
| 500 | Internal server error while getting encoding id |  |

### /groups/{groupName}/encodings/{encodingId}

#### GET
##### Description:

Get the encoding information corresponding to the encoding id.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| encodingId | path | Encoding id that identifies a unique combination of schema and codec type | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Encoding | [EncodingInfo](#encodinginfo) |
| 404 | Group or encoding id with given name not found |  |
| 500 | Internal server error while getting encoding info corresponding to encoding id |  |

### /groups/{groupName}/codecTypes

#### GET
##### Description:

Get codecTypes for the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found CodecTypes | [CodecTypesList](#codectypeslist) |
| 404 | Group or encoding id with given name not found |  |
| 500 | Internal server error while fetching codecTypes registered |  |

#### POST
##### Description:

Adds a new codecType to the group.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| namespace | query | Namespace in which to lookup group. If no namespace is specified, default namespace is used. | No | string |
| codecType | body | The codecType | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 201 | Successfully added codecType to group |
| 404 | Group not found |
| 500 | Internal server error while registering codectype to a Group |

### /schemas/addedTo

#### POST
##### Description:

Gets a map of groups to version info where the schema if it is registered. SchemaInfo#properties is ignored while comparing the schema.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| namespace | query | Namespace in which to lookup schemas used in groups. If no namespace is specified, default namespace is used. | No | string |
| schemaInfo | body | Get schema references for the supplied schema | Yes | [SchemaInfo](#schemainfo) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema version | [AddedTo](#addedto) |
| 404 | Schema not found |  |
| 500 | Internal server error while fetching Schema references |  |

### Models


#### ListGroupsResponse

Map of Group names to group properties. For partially created groups, the group properties may be null.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| groups | object |  | No |
| continuationToken | string | Continuation token to identify the position of last group in the response. | Yes |

#### GroupProperties

Metadata for a group.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| serializationFormat | [SerializationFormat](#serializationformat) | serialization format for the group. | Yes |
| compatibility | [Compatibility](#compatibility) | Compatibility to apply while registering new schema. | Yes |
| allowMultipleTypes | boolean | Flag to indicate whether to allow multiple schemas representing distinct objects to be registered in the group. | Yes |
| properties | object | User defined Key value strings. | No |

#### SerializationFormat

Serialization format enum that lists different serialization formats supported by the service. To use additional formats, use serializationFormat.Custom and supply fullTypeName.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| serializationFormat | string |  | Yes |
| fullTypeName | string |  | No |

#### SchemaInfo

Schema information object that encapsulates various properties of a schema.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| type | string | Name of the schema. This identifies the type of object the schema payload represents. | Yes |
| serializationFormat | [SerializationFormat](#serializationformat) | Type of schema. | Yes |
| schemaData | binary | Base64 encoded string for binary data for schema. | Yes |
| properties | object | User defined key value strings. | No |

#### VersionInfo

Version information object.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| type | string | Type of schema for this version. This is same value used in SchemaInfo#Type for the schema this version identifies. | Yes |
| version | integer | Version number that uniquely identifies the schema version among all schemas in the group that share the same Type. | Yes |
| id | integer | schema id that uniquely identifies schema version and describes the absolute order in which the schema was added to the group. | Yes |

#### SchemaWithVersion

Object that encapsulates SchemaInfo and its corresponding VersionInfo objects.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information. | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Version information. | Yes |

#### SchemaVersionsList

List of schemas with their versions.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemas | [ [SchemaWithVersion](#schemawithversion) ] | List of schemas with their versions. | No |

#### EncodingId

Encoding id that uniquely identifies a schema version and codec type pair.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| encodingId | integer | encoding id generated by service. | Yes |

#### EncodingInfo

Encoding information object that resolves the schema version and codec type used for corresponding encoding id.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information object. | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Version information object. | Yes |
| codecType | string | Codec type. | Yes |

#### Compatibility

Compatibility policy.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| policy | string | Compatibility policy enum. | Yes |
| advanced | [BackwardAndForward](#backwardandforward) | Backward and forward policy details. | No |

#### BackwardAndForward

BackwardPolicy and forwardPolicy policy.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| backwardPolicy | [BackwardPolicy](#backwardpolicy) | BackwardAndForward policy type that describes different types of BackwardPolicy policies like Backward, BackwardTransitive and BackwardTill. | No |
| forwardPolicy | [ForwardPolicy](#forwardpolicy) | BackwardAndForward policy type that describes different types of ForwardPolicy policies like Forward, ForwardTransitive and ForwardTill. | No |

#### BackwardPolicy

BackwardPolicy policy.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| backwardPolicy |  | BackwardAndForward type backwardPolicy. Can be one of Backward, backwardTill and backwardTransitive. | Yes |

#### ForwardPolicy

ForwardPolicy policy.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| forwardPolicy |  | BackwardAndForward type forwardPolicy. Can be one of forward, forwardTill and forwardTransitive. | Yes |

#### Backward

BackwardPolicy compatibility type which tells the service to check for backwardPolicy compatibility with latest schema.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |

#### Forward

ForwardPolicy compatibility type which tells the service to check for forwardPolicy compatibilty with latest schema.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |

#### BackwardTransitive

BackwardPolicy compatibility type which tells the service to check for backwardPolicy compatibility with all previous schemas.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |

#### ForwardTransitive

ForwardPolicy compatibility type which tells the service to check for forwardPolicy compatibility with all previous schemas.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |

#### BackwardTill

BackwardPolicy compatibility which tells the service to check for backwardPolicy compatibility with all previous schemas till specific version.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Whether given schema is valid with respect to existing group schemas against the configured compatibility. | Yes |

#### ForwardTill

ForwardPolicy compatibility which tells the service to check for forwardPolicy compatibility with all previous schemas till specific version.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Whether given schema is valid with respect to existing group schemas against the configured compatibility. | Yes |

#### CodecTypesList

Response object for listCodecTypes.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| codecTypes | [ string ] | List of codecTypes. | No |

#### Valid

Response object for validateSchema api.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| valid | boolean | Whether given schema is valid with respect to existing group schemas against the configured compatibility. | Yes |

#### CanRead

Response object for canRead api.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| compatible | boolean | Whether given schema is compatible and can be used for reads. BackwardAndForward is checked against existing group schemas subject to group's configured compatibility policy. | Yes |

#### GroupHistoryRecord

Group History Record that describes each schema evolution - schema information, version generated for the schema, time and compatibility policy used for schema validation.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information object. | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Schema version information object. | Yes |
| compatibility | [Compatibility](#compatibility) | Schema compatibility applied. | Yes |
| timestamp | long | Timestamp when the schema was added. | Yes |
| schemaString | string | Schema as json string for serialization formats that registry service understands. | No |

#### GroupHistory

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| history | [ [GroupHistoryRecord](#grouphistoryrecord) ] | Chronological list of Group History records. | No |

#### AddedTo

Map of Group names to versionInfos in the group. This is for all the groups where the schema is registered.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| groups | object | Version for the schema in the group. | Yes |

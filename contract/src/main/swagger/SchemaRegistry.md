# Pravega Schema Registry APIs
REST APIs for Pravega Schema Registry.

## Version: 0.0.1

**License:** [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

### /groups

#### GET
##### Description:

List all groups

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
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

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Group history | [GroupHistory](#grouphistory) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/rules

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Group schema validation rules | [SchemaValidationRules](#schemavalidationrules) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

#### PUT
##### Description:

update schema validation rules of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| UpdateValidationRulesPolicyRequest | body | update group policy | Yes | object |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 | Updated schema validation policy |
| 404 | Group with given name not found |
| 409 | Write conflict |
| 500 | Internal server error while fetching Group details |

### /groups/{groupName}/schemas

#### GET
##### Description:

Fetch all different objects identified by schema names under a Group. This api will return schema types.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | List of objects identified by schema names under the group | [SchemaNamesList](#schemanameslist) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/schemas/versions

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Versioned history of schemas registered under the group | [SchemaVersionsList](#schemaversionslist) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

#### POST
##### Description:

Adds a new schema to the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| AddSchemaToGroupRequest | body | Add new schema to group | Yes | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 201 | Successfully added schema to the group | [VersionInfo](#versioninfo) |
| 404 | Group not found |  |
| 409 | Incompatible schema |  |
| 417 | Invalid schema type |  |
| 500 | Internal server error while creating a Group |  |

### /groups/{groupName}/schemas/versions/search

#### POST
##### Description:

Get the version for the schema if it is registered. It does not automatically register the schema. To add new schema use addSchemaToGroup

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| GetSchemaVersion | body | Get schema corresponding to the version | Yes | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema version | [VersionInfo](#versioninfo) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/schemas/versions/{versionOrdinal}

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| version | path | Version ordinal | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Schema corresponding to the version | [SchemaInfo](#schemainfo) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/schemas/versions/latest

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Group properties | [SchemaWithVersion](#schemawithversion) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/schemas/versions/validate

#### POST
##### Description:

Checks if given schema is compatible with schemas in the registry for current policy setting.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| ValidateRequest | body | Checks if schema is valid with respect to supplied validation rules | Yes | object |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 | Schema is valid |
| 404 | Group with given name not found |
| 500 | Internal server error while fetching Group details |

### /groups/{groupName}/schemas/versions/canRead

#### POST
##### Description:

Checks if given schema can be used for reads subject to compatibility policy in the schema validation rules.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| CanReadRequest | body | Checks if schema can be used to read the data in the stream based on compatibility rules. | Yes | object |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 | Schema can be used to read |
| 404 | Group with given name not found |
| 500 | Internal server error while fetching Group details |

### /groups/{groupName}/schemas/{schemaName}/versions

#### GET
##### Description:

Fetch all schemas registered with the given schema name

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| schemaName | path | Schema name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Versioned history of schemas registered under the group of specified schema type | [SchemaVersionsList](#schemaversionslist) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/schemas/{schemaName}/versions/latest

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| schemaName | path | Schema name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found latest schema in name | [SchemaWithVersion](#schemawithversion) |
| 404 | Group with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/encodings

#### PUT
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| GetEncodingIdRequest | body | Get schema corresponding to the version | Yes | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Encoding | [EncodingId](#encodingid) |
| 404 | Group or encoding id with given name not found |  |
| 412 | Codec not registered |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/encodings/{encodingId}

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| encodingId | path | Encoding id that identifies a unique combination of schema and codec | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Encoding | [EncodingInfo](#encodinginfo) |
| 404 | Group or encoding id with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

### /groups/{groupName}/codecs

#### GET
##### Description:

Fetch the properties of an existing Group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Found Codecs | [CodecsList](#codecslist) |
| 404 | Group or encoding id with given name not found |  |
| 500 | Internal server error while fetching Group details |  |

#### POST
##### Description:

Adds a new codec to the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| groupName | path | Group name | Yes | string |
| AddCodec | body | The codec | Yes | object |

##### Responses

| Code | Description |
| ---- | ----------- |
| 201 | Successfully added codec to group |
| 404 | Group not found |
| 500 | Internal server error while creating a Group |

### Models


#### ListGroupsResponse

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| groups | object |  | No |
| continuationToken | string |  | Yes |

#### SchemaNamesList

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| objects | [ string ] |  | No |

#### GroupProperties

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaValidationRules | [SchemaValidationRules](#schemavalidationrules) |  | Yes |
| schemaType | [SchemaType](#schematype) |  | Yes |
| versionBySchemaName | boolean |  | Yes |
| properties | object |  | No |

#### SchemaType

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaType | string |  | Yes |
| customTypeName | string |  | No |

#### SchemaInfo

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaName | string |  | Yes |
| schemaType | [SchemaType](#schematype) |  | Yes |
| schemaData | binary |  | Yes |
| properties | object |  | No |

#### VersionInfo

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaName | string |  | Yes |
| version | integer |  | Yes |
| ordinal | integer |  | Yes |

#### SchemaWithVersion

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) |  | Yes |
| version | [VersionInfo](#versioninfo) |  | Yes |

#### SchemaVersionsList

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemas | [ [SchemaWithVersion](#schemawithversion) ] |  | No |

#### CodecType

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| codecType | string |  | Yes |
| customTypeName | string |  | No |
| properties | object |  | No |

#### EncodingId

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| encodingId | integer |  | Yes |

#### EncodingInfo

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) |  | Yes |
| versionInfo | [VersionInfo](#versioninfo) |  | Yes |
| codecType | [CodecType](#codectype) |  | Yes |

#### Compatibility

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| policy | string |  | Yes |
| backwardTill | [VersionInfo](#versioninfo) |  | No |
| forwardTill | [VersionInfo](#versioninfo) |  | No |

#### SchemaValidationRules

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| rules | object |  | No |

#### SchemaValidationRule

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| rule |  |  | Yes |

#### CodecsList

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| codecTypes | [ [CodecType](#codectype) ] |  | No |

#### Valid

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| valid | boolean |  | Yes |

#### CanRead

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| compatible | boolean |  | Yes |

#### GroupHistoryRecord

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) |  | Yes |
| version | [VersionInfo](#versioninfo) |  | Yes |
| validationRules | [SchemaValidationRules](#schemavalidationrules) |  | Yes |
| timestamp | long |  | Yes |
| schemaString | string |  | No |

#### GroupHistory

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| history | [ [GroupHistoryRecord](#grouphistoryrecord) ] |  | No |
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

Map of Group names to group properties. For partially created groups, the group properties may be null.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| groups | object |  | No |
| continuationToken | string | Continuation token to identify the position of last group in the response. | Yes |

#### SchemaNamesList

List of schema names. Schema names uniquely identify different object types under a group.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| objects | [ string ] |  | No |

#### GroupProperties

Metadata for a group.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaType | [SchemaType](#schematype) | Schema type for the group. | Yes |
| schemaValidationRules | [SchemaValidationRules](#schemavalidationrules) | Validation rules to apply while registering new schema. | Yes |
| versionBySchemaName | boolean | Flag to indicate whether to version schemas within the group by schema name. If set to true, addSchema will only validate against schemas that have the same schema name. | Yes |
| properties | object | User defined Key value strings. | No |

#### SchemaType

Schema type enum that lists different schema types supported by the service. To use additional Schema Type, use schemaType.Custom and supply customTypeName.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaType | string |  | Yes |
| customTypeName | string |  | No |

#### SchemaInfo

Schema information object that encapsulates various properties of a schema.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaName | string | Name of the schema. This identifies the type of object the schema payload represents. | Yes |
| schemaType | [SchemaType](#schematype) | Type of schema. | Yes |
| schemaData | binary | Base64 encoded string for binary data for schema. | Yes |
| properties | object | User defined key value strings. | No |

#### VersionInfo

Version information object.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaName | string | Name of schema for this version. This is the name used in SchemaInfo.schemaName. | Yes |
| version | integer | Version number that uniquely identifies the schema version among all schemas in the group that share the same SchemaName. | Yes |
| ordinal | integer | Version ordinal that uniquely identifies the position of the corresponding schema across all schemas in the group. | Yes |

#### SchemaWithVersion

Object that encapsulates SchemaInfo and its corresponding VersionInfo objects.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information. | Yes |
| version | [VersionInfo](#versioninfo) | Version information. | Yes |

#### SchemaVersionsList

List of schemas with their versions.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemas | [ [SchemaWithVersion](#schemawithversion) ] | List of schemas with their versions. | No |

#### CodecType

Type of codec. For custom codec use codecType.Custom with customTypeName and optionally additional properties.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| codecType | string | Code Type enum. | Yes |
| customTypeName | string | Custom type name when codecType.custom is chosen. | No |
| properties | object | Optional additional key value string for codecType.cusom. | No |

#### EncodingId

Encoding id that uniquely identifies a schema version and codec pair.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| encodingId | integer | encoding id generated by service. | Yes |

#### EncodingInfo

Encoding information object that resolves the schema version and codec used for corresponding encoding id.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information object. | Yes |
| versionInfo | [VersionInfo](#versioninfo) | Version information object. | Yes |
| codecType | [CodecType](#codectype) | Codec type object. | Yes |

#### Compatibility

Schema Compatibility validation rule.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string | Name is used to identify the type of SchemaValidationRule. For Compatibility rule the name should be "Compatibility". | Yes |
| policy | string | Compatibility policy enum. | Yes |
| backwardTill | [VersionInfo](#versioninfo) | Version for backward till if policy is BackwardTill or BackwardAndForwardTill. | No |
| forwardTill | [VersionInfo](#versioninfo) | Version for forward till if policy is ForwardTill or BackwardAndForwardTill. | No |

#### SchemaValidationRules

Schema validation rules to be applied for new schema addition. Currently only one rule is supported - Compatibility.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| rules | object |  | No |

#### SchemaValidationRule

Schema validation rule base class.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| rule |  | Specific schema validation rule. The only rule we have presently is Compatibility. The "name" is used to identify specific Rule type. The only rule supported in this is Compatibility. | Yes |

#### CodecsList

Response object for listCodecTypes.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| codecTypes | [ [CodecType](#codectype) ] | List of codecTypes. | No |

#### Valid

Response object for validateSchema api.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| valid | boolean | Whether given schema is valid with respect to existing group schemas against the configured validation rules. | Yes |

#### CanRead

Response object for canRead api.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| compatible | boolean | Whether given schema is compatible and can be used for reads. Compatibility is checked against existing group schemas subject to group's configured compatibility policy. | Yes |

#### GroupHistoryRecord

Group History Record that describes each schema evolution - schema information, version generated for the schema, time and rules used for schema validation.

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schemaInfo | [SchemaInfo](#schemainfo) | Schema information object. | Yes |
| version | [VersionInfo](#versioninfo) | Schema version information object. | Yes |
| validationRules | [SchemaValidationRules](#schemavalidationrules) | Schema validation rules applied. | Yes |
| timestamp | long | Timestamp when the schema was added. | Yes |
| schemaString | string | Schema as json string for schema types that registry service understands. | No |

#### GroupHistory

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| history | [ [GroupHistoryRecord](#grouphistoryrecord) ] | Chronological list of Group History records. | No |
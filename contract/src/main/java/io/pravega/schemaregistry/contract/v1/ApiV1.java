/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.v1;

import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.ObjectsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

public class ApiV1 {
    @Path("/v1/groups")
    @io.swagger.annotations.Api(description = "the groups API")
    public interface GroupsApi {
        @POST
        @Path("/{groupName}/codecs")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new codec to the group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added codec to group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        Response addCodec(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                          @ApiParam(value = "The codec", required = true) AddCodec addCodec);

        @POST
        @Path("/{groupName}/schemas")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 417, message = "Invalid schema type", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        Response addSchemaToGroupIfAbsent(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                          @ApiParam(value = "Add new schema to group", required = true) AddSchemaToGroupRequest addSchemaToGroupRequest);

        @POST
        @Path("/{groupName}/schemas/canRead")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = Void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema can be used to read", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response canRead(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                         @ApiParam(value = "Checks if schema can be used to read the data in the stream based on compatibility rules.", required = true) CanReadRequest canReadRequest);

        @POST
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        Response createGroup(@ApiParam(value = "The Group configuration", required = true) CreateGroupRequest createGroupRequest);

        @DELETE
        @Path("/{groupName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class)})
        Response deleteGroup(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/codecs")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CodecsList.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Codecs", response = CodecsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getCodecsList(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/encodings/{encodingId}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getEncodingInfo(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                 @ApiParam(value = "Encoding id that identifies a unique combination of schema and codec", required = true) @PathParam("encodingId") Integer encodingId);

        @GET
        @Path("/{groupName}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getGroupProperties(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/schemas/versions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getGroupSchemas(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/schemas/versions/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getLatestGroupSchema(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/objects/{schemaName}/schemas/versions/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in name", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public Response getLatestSchemaForSchemaName(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                               @ApiParam(value = "Object type", required = true) @PathParam("schemaName") String schemaName);

        @GET
        @Path("/{groupName}/objects/{objectName}/schemas/versions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered with the given schema name", response = SchemaList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group of specified schema type", response = SchemaList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public Response getObjectSchemas(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                         @ApiParam(value = "Object type", required = true) @PathParam("objectName") String objectName);

        @GET
        @Path("/{groupName}/objects")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all object types under a Group. This api will return schema types.", response = ObjectsList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of object types under the group", response = ObjectsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public Response getObjects(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @PUT
        @Path("/{groupName}/encodings")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingId.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 412, message = "Codec not registered", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getEncodingId(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                         @ApiParam(value = "Get schema corresponding to the version", required = true) GetEncodingIdRequest getEncodingIdRequest);

        @GET
        @Path("/{groupName}/schemas/versions/{version}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaFromVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                      @ApiParam(value = "version ordinal", required = true) @PathParam("version") Integer version);

        @GET
        @Path("/{groupName}/rules")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaValidationRules.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group schema validation rules", response = SchemaValidationRules.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaValidationRules(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @POST
        @Path("/{groupName}/schemas/versions/schema")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered.", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                  @ApiParam(value = "Get schema corresponding to the version", required = true) GetSchemaVersion getSchemaVersion);

        @GET
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups", response = GroupsList.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = GroupsList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class)})
        Response listGroups();

        @PUT
        @Path("/{groupName}/rules")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Write conflict", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response updateSchemaValidationRules(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                             @ApiParam(value = "update group policy", required = true) UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest);

        @POST
        @Path("/{groupName}/schemas/validate")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response validate(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                          @ApiParam(value = "Checks if schema is valid with respect to supplied validation rules", required = true) ValidateRequest validateRequest);
    }
}

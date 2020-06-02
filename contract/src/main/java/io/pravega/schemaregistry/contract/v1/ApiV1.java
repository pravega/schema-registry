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
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class ApiV1 {
    @Path("/ping")
    public interface Ping {
        @GET
        Response ping();
    }

    /**
     * Sync Group apis. Identical to {@link GroupsApiAsync}. All methods in this interface are synchronou and return {@link Response} object.
     * The purposes of this interface is to be used by proxy-client.
     */
    @Path("/v1/groups")
    @io.swagger.annotations.Api(description = "the groups API")
    public interface GroupsApi {
        @POST
        @Path("/{groupName}/codecs")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new codec to the group", response = Void.class, tags = {"Codecs", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added codec to group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        Response addCodec(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                          @ApiParam(value = "The codec", required = true) AddCodec addCodec);

        @POST
        @Path("/{groupName}/schemas/versions")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 417, message = "Invalid serialization format", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        Response addSchema(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                           @ApiParam(value = "Add new schema to group", required = true) AddSchemaRequest addSchemaRequest);

        @POST
        @Path("/{groupName}/schemas/versions/canRead")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = CanRead.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Response to tell whether schema can be used to read existing schemas", response = CanRead.class),

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
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get codecs for the group", response = CodecsList.class, tags = {"Codecs", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Codecs", response = CodecsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getCodecsList(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/encodings/{encodingId}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get the encoding information corresponding to the encoding id.", response = EncodingInfo.class, tags = {"Encoding", })
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
        @Path("/{groupName}/history")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the history of schema evolution of a Group", response = GroupHistory.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group history", response = GroupHistory.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public Response getGroupHistory(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @GET
        @Path("/{groupName}/schemas/versions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get all schema versions for the group", response = SchemaVersionsList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaVersionsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaVersions(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                   @ApiParam(value = "Type") @QueryParam("type") String type);

        @GET
        @Path("/{groupName}/schemas/versions/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get latest schema for the group.", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getLatestSchema(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                 @ApiParam(value = "Type") @QueryParam("type") String type);

        @GET
        @Path("/{groupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch latest schema versions for all objects identified by SchemaInfo#type under a Group.", response = SchemaVersionsList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Latest schemas for all objects identified by SchemaInfo#type under the group", response = SchemaVersionsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public Response getSchemas(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @PUT
        @Path("/{groupName}/encodings")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get an encoding id that uniquely identifies a schema version and codec pair.", response = EncodingId.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 412, message = "Codec not registered", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getEncodingId(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                               @ApiParam(value = "Get schema corresponding to the version", required = true) GetEncodingIdRequest getEncodingIdRequest);

        @GET
        @Path("/{groupName}/schemas/versions/{versionOrdinal}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete schema version from the group.", response = Void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response deleteSchemaVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                     @ApiParam(value = "version ordinal", required = true) @PathParam("versionOrdinal") Integer version);

        @DELETE
        @Path("/{groupName}/schemas/versions/{versionOrdinal}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get schema from the version ordinal that uniquely identifies the schema in the group.", response = SchemaInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Schema version deleted.", response = SchemaInfo.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaFromVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                      @ApiParam(value = "version ordinal", required = true) @PathParam("versionOrdinal") Integer version);

        @GET
        @Path("/{groupName}/rules")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the schema validation rules configured for the Group", response = SchemaValidationRules.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group schema validation rules", response = SchemaValidationRules.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response getSchemaValidationRules(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName);

        @POST
        @Path("/{groupName}/schemas/versions/find")
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
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups", response = ListGroupsResponse.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = ListGroupsResponse.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class)})
        Response listGroups(@ApiParam(value = "Continuation token") @QueryParam("continuationToken") String continuationToken,
                            @ApiParam(value = "The numbers of items to return") @QueryParam("limit") Integer limit);

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
                                             @ApiParam(value = "update group policy", required = true) UpdateValidationRulesRequest updateValidationRulesRequest);

        @POST
        @Path("/{groupName}/schemas/versions/validate")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Valid.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema validation response", response = Valid.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        Response validate(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                          @ApiParam(value = "Checks if schema is valid with respect to supplied validation rules", required = true) ValidateRequest validateRequest);
    }

    /**
     * ASync Group apis. Identical to {@link GroupsApi}. All methods in this interface are asynchronous and use
     * {@link AsyncResponse}. This is used on service side so that all api implementation is asynchronous.
     */
    @Path("/v1/groups")
    @io.swagger.annotations.Api(description = "the groups API")
    public interface GroupsApiAsync {
        @POST
        @Path("/{groupName}/codecs")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new codec to the group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added codec to group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        void addCodec(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                      @ApiParam(value = "The codec", required = true) AddCodec addCodec, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @POST
        @Path("/{groupName}/schemas/versions")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 417, message = "Invalid serialization format", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        void addSchema(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                       @ApiParam(value = "Add new schema to group", required = true) AddSchemaRequest addSchemaRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @POST
        @Path("/{groupName}/schemas/versions/canRead")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = CanRead.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Response to tell whether schema can be used to read existing schemas", response = CanRead.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void canRead(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                     @ApiParam(value = "Checks if schema can be used to read the data in the stream based on compatibility rules.", required = true) CanReadRequest canReadRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @POST
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class)})
        void createGroup(@ApiParam(value = "The Group configuration", required = true) CreateGroupRequest createGroupRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @DELETE
        @Path("/{groupName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class)})
        void deleteGroup(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/codecs")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get codecs for the group.", response = CodecsList.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Codecs", response = CodecsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getCodecsList(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/encodings/{encodingId}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get the encoding information corresponding to the encoding id.", response = EncodingInfo.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getEncodingInfo(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                             @ApiParam(value = "Encoding id that identifies a unique combination of schema and codec", required = true) @PathParam("encodingId") Integer encodingId, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getGroupProperties(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/history")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the history of schema evolution of a Group.", response = GroupHistory.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group history", response = GroupHistory.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public void getGroupHistory(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/schemas/versions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get all schema versions for the group.", response = SchemaVersionsList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaVersionsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getSchemaVersions(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                               @ApiParam(value = "Type") @QueryParam("type") String type,
                               @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/schemas/versions/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get latest schema for the group.", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getLatestSchema(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                             @ApiParam(value = "Type") @QueryParam("type") String type,
                             @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch latest schema versions for all objects identified by SchemaInfo#type under a Group.", response = SchemaVersionsList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Latest schemas for all objects identified by SchemaInfo#type under the group", response = SchemaVersionsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        public void getSchemas(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @PUT
        @Path("/{groupName}/encodings")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get an encoding id that uniquely identifies a schema version and codec pair.", response = EncodingId.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 412, message = "Codec not registered", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getEncodingId(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                           @ApiParam(value = "Get schema corresponding to the version", required = true) GetEncodingIdRequest getEncodingIdRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/schemas/versions/{versionOrdinal}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get schema from the version ordinal that uniquely identifies the schema in the group.", response = SchemaInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getSchemaFromVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                  @ApiParam(value = "version ordinal", required = true) @PathParam("versionOrdinal") Integer version, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @DELETE
        @Path("/{groupName}/schemas/versions/{versionOrdinal}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete schema version from the group.", response = Void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Delete schema version from the group", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void deleteSchemaVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                 @ApiParam(value = "version ordinal", required = true) @PathParam("versionOrdinal") Integer version, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Path("/{groupName}/rules")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the schema validation rules configured for the Group", response = SchemaValidationRules.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group schema validation rules", response = SchemaValidationRules.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getSchemaValidationRules(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @POST
        @Path("/{groupName}/schemas/versions/find")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered.", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void getSchemaVersion(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                              @ApiParam(value = "Get schema corresponding to the version", required = true) GetSchemaVersion getSchemaVersion,
                              @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @GET
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups", response = ListGroupsResponse.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = ListGroupsResponse.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class)})
        void listGroups(@ApiParam(value = "Continuation token") @QueryParam("continuationToken") String continuationToken,
                        @ApiParam(value = "The numbers of items to return") @QueryParam("limit") Integer limit, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @PUT
        @Path("/{groupName}/rules")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Write conflict", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void updateSchemaValidationRules(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                         @ApiParam(value = "update group policy", required = true) UpdateValidationRulesRequest updateValidationRulesRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;

        @POST
        @Path("/{groupName}/schemas/versions/validate")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Valid.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema validation response", response = Valid.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class)})
        void validate(@ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                      @ApiParam(value = "Checks if schema is valid with respect to supplied validation rules", required = true) ValidateRequest validateRequest, @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse) throws NotFoundException;
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.schemaregistry.server.rest.generated.api.NotFoundException;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToSubgroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CanReadUsingSchemaRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CheckCompatibilityRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionsList;
import io.pravega.schemaregistry.server.rest.generated.model.CreateGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CreateScopeRequest;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingInfo;
import io.pravega.schemaregistry.server.rest.generated.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GroupProperty;
import io.pravega.schemaregistry.server.rest.generated.model.GroupsList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersion;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersionList;
import io.pravega.schemaregistry.server.rest.generated.model.ScopesList;
import io.pravega.schemaregistry.server.rest.generated.model.UpdateCompatibilityPolicyRequest;
import io.pravega.schemaregistry.server.rest.generated.model.VersionInfo;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Controller APIs exposed via REST.
 * Different interfaces will hold different groups of APIs.
 *
 * ##############################IMPORTANT NOTE###################################
 * Do not make any API changes here directly, you need to update swagger/Controller.yaml and generate
 * the server stubs as documented in swagger/README.md before updating this file.
 *
 */
public final class ApiV1 {

    @Path("/ping")
    public interface Ping {
        @GET
        Response ping();
    }

    /**
     * Stream metadata version 1.0 APIs.
     */
    @Path("/v1/scopes")
    @io.swagger.annotations.Api(description = "the scopes API")
    public interface ScopesApi {

        @POST
        @Path("/{scopeName}/Groups/{GroupName}/schemas")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
        public void addSchemaToGroupIfAbsent(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaToGroupRequest addSchemaRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @POST
        @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to subgroup", response = VersionInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
        public void addSchemaToSubgroupIfAbsent(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
                ,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaToSubgroupRequest addSchemaRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/schemas/canRead")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Compatibility check", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void canRead(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Get schema is compatible with existing schemas for existing compatibility setting" ,required=true) CanReadUsingSchemaRequest canReadUsingSchemaRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/schemas/compatibility")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is compatible", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void checkCompatibility(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Get schema is compatible with existing schemas for existing compatibility setting" ,required=true) CheckCompatibilityRequest checkCompatibilityRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @POST
        @Path("/{scopeName}/groups")
        @Consumes({ "application/json" })

        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Groups", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group to the namespace", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
        public void createGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @POST

        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Scope", response = Void.class, tags={ "Scope", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the Scope", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 409, message = "Scope with the given name already exists", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a scope", response = Void.class) })
        public void createScope(@ApiParam(value = "Create scope" ,required=true) CreateScopeRequest createScopeRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @DELETE
        @Path("/{scopeName}/Groups/{GroupName}")


        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
        public void deleteGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/compressions")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsList.class, tags={ "Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsList.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getCompressionsList(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/encodings/{EncodingId}")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getEncodingId(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression",required=true) @PathParam("EncodingId") String encodingId
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperty.class, tags={ "Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperty.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getGroupProperties(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/schemas")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersionList.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaWithVersionList.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getGroupSchemas(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/schemas/latest")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getLatestGroupSchema(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas/latest")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in subgroup", response = SchemaWithVersion.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getLatestSubgroupSchema(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @PUT
        @Path("/{scopeName}/Groups/{GroupName}/encodings")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getOrGenerateEncodingId(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas/version")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getSchemaFromSubgroupVersion(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
                ,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/schemas/version")
        @Consumes({ "application/json" })
        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getSchemaFromVersion(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/Groups/{GroupName}/subgroups/{SubgroupName}/schemas")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered under a sub Group", response = SchemaWithVersionList.class, tags={ "Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaWithVersionList.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void getSubGroupSchemas(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "Subgroup name",required=true) @PathParam("SubgroupName") String subgroupName
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET
        @Path("/{scopeName}/groups")

        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "List groups within the given scope", response = GroupsList.class, tags={ "Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups under the given scope namespace", response = GroupsList.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups for the given scope", response = Void.class) })
        public void listGroups(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "") @QueryParam("") String ERROR_UNKNOWN
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @GET


        @Produces({ "application/json" })
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all available Scopes in schema registry", response = ScopesList.class, tags={ "Scope", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of currently available Scopes", response = ScopesList.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching list of scopes", response = Void.class) })
        public void listScopes(@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
        @PUT
        @Path("/{scopeName}/Groups/{GroupName}")
        @Consumes({ "application/json" })

        @io.swagger.annotations.ApiOperation(value = "", notes = "update compatibility policy of an existing Group", response = Void.class, tags={ "Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Updated compatibility policy", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = Void.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
        public void updateCompatibilityPolicy(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
                ,@ApiParam(value = "Group name",required=true) @PathParam("GroupName") String groupName
                ,@ApiParam(value = "update group policy" ,required=true) UpdateCompatibilityPolicyRequest updateCompatibilityPolicyRequest
                ,@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateScopeRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingIdModel;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupPropertiesModel;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.ScopesListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersionModel;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfoModel;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
        @Path("/{scopeName}/groups/{groupName}/schemas")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfoModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfoModel.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = VersionInfoModel.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = VersionInfoModel.class)})
        public void addSchemaToGroupIfAbsent(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName, 
                                             @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName, 
                                             @ApiParam(value = "Add new schema to group", required = true) AddSchemaToGroupRequest addSchemaToGroupRequest, 
                                             @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/schemas/validate")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = void.class)})
        public void validate(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Checks if schema is valid with respect to supplied validation rules", required = true) ValidateRequest validateRequest,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @POST
        @Path("/{scopeName}/groups")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = void.class, tags = {"Groups", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group to the namespace", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = void.class)})
        public void createGroup(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "The Group configuration", required = true) CreateGroupRequest createGroupRequest,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @POST
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Scope", response = void.class, tags = {"Scope", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the Scope", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Scope with the given name already exists", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a scope", response = void.class)})
        public void createScope(@ApiParam(value = "Create scope", required = true) CreateScopeRequest createScopeRequest,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @DELETE
        @Path("/{scopeName}/groups/{groupName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = void.class)})
        public void deleteGroup(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @DELETE
        @Path("/{scopeName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a scope", response = void.class, tags = {"Scope", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the scope", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete scope since it has non-empty list of Groups", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a scope", response = void.class)})
        public void deleteScope(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/compressions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsListModel.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsListModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = CompressionsListModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = CompressionsListModel.class)})
        public void getCompressionsList(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                                        @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                                        @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/encodings/{EncodingIdModel}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfoModel.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfoModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = EncodingInfoModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = EncodingInfoModel.class)})
        public void getEncodingInfo(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                                    @ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                    @ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression", required = true) @PathParam("EncodingIdModel") Integer encodingId,
                                    @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupPropertiesModel.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupPropertiesModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = GroupPropertiesModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = GroupPropertiesModel.class)})
        public void getGroupProperties(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaEvolutionListModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionListModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = SchemaEvolutionListModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaEvolutionListModel.class)})
        public void getGroupSchemas(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/schemas/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersionModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersionModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = SchemaWithVersionModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaWithVersionModel.class)})
        public void getLatestGroupSchema(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/subgroups/{subgroupName}/schemas/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersionModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in subgroup", response = SchemaWithVersionModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = SchemaWithVersionModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaWithVersionModel.class)})
        public void getLatestSubgroupSchema(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Subgroup name", required = true) @PathParam("SubgroupName") String subgroupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @PUT
        @Path("/{scopeName}/groups/{groupName}/encodings")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingIdModel.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingIdModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group or encoding id with given name not found", response = EncodingIdModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = EncodingIdModel.class)})
        public void getOrGenerateEncodingId(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                                            @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                                            @ApiParam(value = "Get schema corresponding to the version", required = true) GetEncodingIdRequest getEncodingIdModelRequest,
                                            @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/schemas/version")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfoModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfoModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = SchemaInfoModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaInfoModel.class)})
        public void getSchemaFromVersion(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Get schema corresponding to the version", required = true) GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups/{groupName}/subgroups/{subgroupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered under a sub Group", response = SchemaEvolutionListModel.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionListModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = SchemaEvolutionListModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaEvolutionListModel.class)})
        public void getSubGroupSchemas(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Subgroup name", required = true) @PathParam("SubgroupName") String subgroupName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{scopeName}/groups")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List groups within the given scope", response = GroupsListModel.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups under the given scope namespace", response = GroupsListModel.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = GroupsListModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups for the given scope", response = GroupsListModel.class)})
        public void listGroups(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all available Scopes in schema registry", response = ScopesListModel.class, tags = {"Scope", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of currently available Scopes", response = ScopesListModel.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching list of scopes", response = ScopesListModel.class)})
        public void listScopes(@Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;

        @PUT
        @Path("/{scopeName}/groups/{groupName}")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or Group with given name not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = void.class)})
        public void updateSchemaValidationRules(@ApiParam(value = "Scope name", required = true) @PathParam("scopeName") String scopeName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "update group policy", required = true) UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest,
                @Context SecurityContext securityContext, AsyncResponse asyncResponse)
                throws NotFoundException;
    }
}

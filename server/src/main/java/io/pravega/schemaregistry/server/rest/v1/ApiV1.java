/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.v1;

import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateNamespaceRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespacesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
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
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Schema Registry APIs exposed via REST.
 * Different interfaces will hold different groups of APIs.
 * 
 * ##############################IMPORTANT NOTE###################################
 * Do not make any API changes here directly, you need to update swagger/schemaregistry.yaml and generate
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
    @Path("/v1/namespaces")
    @io.swagger.annotations.Api(description = "the namespaces API")
    public interface NamespacesApi {
        @POST
        @Path("/{namespaceName}/groups/{groupName}/schemas")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),

                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = VersionInfo.class),

                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = VersionInfo.class)})
        public void addSchemaToGroupIfAbsent(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName, 
                                             @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName, 
                                             @ApiParam(value = "Add new schema to group", required = true) AddSchemaToGroupRequest addSchemaToGroupRequest, 
                                             @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/schemas/validate")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = void.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = void.class)})
        public void validate(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Checks if schema is valid with respect to supplied validation rules", required = true) ValidateRequest validateRequest,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @POST
        @Path("/{namespaceName}/groups")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = void.class, tags = {"Groups", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group to the namespace", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = void.class)})
        public void createGroup(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "The Group configuration", required = true) CreateGroupRequest createGroupRequest,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @POST
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Namespace", response = void.class, tags = {"Namespace", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the Namespace", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Namespace with the given name already exists", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a namespace", response = void.class)})
        public void createNamespace(@ApiParam(value = "Create namespace", required = true) CreateNamespaceRequest createNamespaceRequest,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @DELETE
        @Path("/{namespaceName}/groups/{groupName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = void.class)})
        public void deleteGroup(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @DELETE
        @Path("/{namespaceName}")
        @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a namespace", response = void.class, tags = {"Namespace", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the namespace", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete namespace since it has non-empty list of Groups", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a namespace", response = void.class)})
        public void deleteNamespace(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/compressions")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsList.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = CompressionsList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = CompressionsList.class)})
        public void getCompressionsList(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                                        @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                                        @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/encodings/{EncodingId}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = EncodingInfo.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = EncodingInfo.class)})
        public void getEncodingInfo(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                                    @ApiParam(value = "Group name", required = true) @PathParam("groupName") String groupName,
                                    @ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression", required = true) @PathParam("EncodingId") Integer encodingId,
                                    @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = GroupProperties.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = GroupProperties.class)})
        public void getGroupProperties(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaEvolutionList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = SchemaEvolutionList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaEvolutionList.class)})
        public void getGroupSchemas(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/schemas/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaWithVersion.class)})
        public void getLatestGroupSchema(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/subgroups/{subgroupName}/schemas/latest")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in subgroup", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = SchemaWithVersion.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaWithVersion.class)})
        public void getLatestSubgroupSchema(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Subgroup name", required = true) @PathParam("SubgroupName") String subgroupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @PUT
        @Path("/{namespaceName}/groups/{groupName}/encodings")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingId.class, tags = {"Encoding", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = EncodingId.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = EncodingId.class)})
        public void getOrGenerateEncodingId(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                                            @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                                            @ApiParam(value = "Get schema corresponding to the version", required = true) GetEncodingIdRequest getEncodingIdRequest,
                                            @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/schemas/version")
        @Consumes({"application/json"})
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = SchemaInfo.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaInfo.class)})
        public void getSchemaFromVersion(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Get schema corresponding to the version", required = true) GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups/{groupName}/subgroups/{subgroupName}/schemas")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered under a sub Group", response = SchemaEvolutionList.class, tags = {"Schema", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = SchemaEvolutionList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = SchemaEvolutionList.class)})
        public void getSubGroupSchemas(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "Subgroup name", required = true) @PathParam("SubgroupName") String subgroupName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Path("/{namespaceName}/groups")
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List groups within the given namespace", response = GroupsList.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups under the given namespace namespace", response = GroupsList.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = GroupsList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups for the given namespace", response = GroupsList.class)})
        public void listGroups(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @GET
        @Produces({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "List all available Namespaces in schema registry", response = NamespacesList.class, tags = {"Namespace", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "List of currently available Namespaces", response = NamespacesList.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching list of namespaces", response = NamespacesList.class)})
        public void listNamespaces(@Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;

        @PUT
        @Path("/{namespaceName}/groups/{groupName}")
        @Consumes({"application/json"})
        @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = void.class, tags = {"Group", })
        @io.swagger.annotations.ApiResponses(value = {
                @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = void.class),
                @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = void.class)})
        public void updateSchemaValidationRules(@ApiParam(value = "Namespace name", required = true) @PathParam("namespaceName") String namespaceName,
                @ApiParam(value = "Group name", required = true) @PathParam("GroupName") String groupName,
                @ApiParam(value = "update group policy", required = true) UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest,
                @Context SecurityContext securityContext, @Suspended AsyncResponse asyncResponse)
                throws NotFoundException;
    }
}

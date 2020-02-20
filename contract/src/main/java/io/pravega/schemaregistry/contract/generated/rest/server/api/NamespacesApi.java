package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NamespacesApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.NamespacesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateNamespaceRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromSubgroupVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespacesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.Map;
import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;
import javax.validation.constraints.*;

@Path("/namespaces")


@io.swagger.annotations.Api(description = "the namespaces API")

public class NamespacesApi  {
   private final NamespacesApiService delegate;

   public NamespacesApi(@Context ServletConfig servletContext) {
      NamespacesApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("NamespacesApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (NamespacesApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = NamespacesApiServiceFactory.getNamespacesApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/{namespaceName}/groups/{groupName}/schemas")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addSchemaToGroupIfAbsent(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaToGroupRequest addSchemaToGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchemaToGroupIfAbsent(namespaceName,groupName,addSchemaToGroupRequest,securityContext);
    }
    @POST
    @Path("/{namespaceName}/groups")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group to the namespace", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createGroup(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createGroup(namespaceName,createGroupRequest,securityContext);
    }
    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Namespace", response = Void.class, tags={ "Namespace", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the Namespace", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Namespace with the given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a namespace", response = Void.class) })
    public Response createNamespace(@ApiParam(value = "Create namespace" ,required=true) CreateNamespaceRequest createNamespaceRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createNamespace(createNamespaceRequest,securityContext);
    }
    @DELETE
    @Path("/{namespaceName}/groups/{groupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
    public Response deleteGroup(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteGroup(namespaceName,groupName,securityContext);
    }
    @DELETE
    @Path("/{namespaceName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a namespace", response = Void.class, tags={ "Namespace", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the namespace", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete namespace since it has non-empty list of Groups", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a namespace", response = Void.class) })
    public Response deleteNamespace(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteNamespace(namespaceName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/compressions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = CompressionsList.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Compressions", response = CompressionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getCompressionsList(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getCompressionsList(namespaceName,groupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/encodings/{encodingId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingInfo.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getEncodingInfo(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Encoding id that identifies a unique combination of encoding and compression",required=true) @PathParam("encodingId") Integer encodingId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingInfo(namespaceName,groupName,encodingId,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupProperties(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupProperties(namespaceName,groupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaEvolutionList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupSchemas(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupSchemas(namespaceName,groupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/schemas/versions/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestGroupSchema(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestGroupSchema(namespaceName,groupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/subgroups/{subgroupName}/schemas/versions/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found latest schema in subgroup", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestSubgroupSchema(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("subgroupName") String subgroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestSubgroupSchema(namespaceName,groupName,subgroupName,securityContext);
    }
    @PUT
    @Path("/{namespaceName}/groups/{groupName}/encodings")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = EncodingId.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getOrGenerateEncodingId(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getOrGenerateEncodingId(namespaceName,groupName,getEncodingIdRequest,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/subgroups/{subgroupName}/schemas/versions/{versionId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromSubgroupVersion(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("subgroupName") String subgroupName
,@ApiParam(value = "version id",required=true) @PathParam("versionId") String versionId
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromSubgroupVersionRequest getSchemaFromSubgroupVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromSubgroupVersion(namespaceName,groupName,subgroupName,versionId,getSchemaFromSubgroupVersionRequest,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/schemas/versions/{versionId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromVersion(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Group name",required=true) @PathParam("versionId") String versionId
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaFromVersionRequest getSchemaFromVersionRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersion(namespaceName,groupName,versionId,getSchemaFromVersionRequest,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/schemas")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered.", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaVersion(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaVersion getSchemaVersion
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaVersion(namespaceName,groupName,getSchemaVersion,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/subgroups/{subgroupName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all schemas registered under a sub Group", response = SchemaEvolutionList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaEvolutionList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSubGroupSchemas(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Subgroup name",required=true) @PathParam("subgroupName") String subgroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSubGroupSchemas(namespaceName,groupName,subgroupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/subgroups")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all subgroups under a Group. Schemas under a group can be subgrouped based on event type. A subgroup denotes a set of schemas that are evolved separately.", response = SchemaEvolutionList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of subgroups under the group", response = SchemaEvolutionList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSubGroups(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSubGroups(namespaceName,groupName,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List groups within the given namespace", response = GroupsList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups under the given namespace namespace", response = GroupsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups for the given namespace", response = Void.class) })
    public Response listGroups(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listGroups(namespaceName,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all available Namespaces in schema registry", response = NamespacesList.class, tags={ "Namespace", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of currently available Namespaces", response = NamespacesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching list of namespaces", response = Void.class) })
    public Response listNamespaces(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listNamespaces(securityContext);
    }
    @PUT
    @Path("/{namespaceName}/groups/{groupName}")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Conflict while attempting to update policy.", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response updateSchemaValidationRules(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "update group policy" ,required=true) UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateSchemaValidationRules(namespaceName,groupName,updateValidationRulesPolicyRequest,securityContext);
    }
    @GET
    @Path("/{namespaceName}/groups/{groupName}/schemas/validate")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "check if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Namespace or Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response validate(@ApiParam(value = "Namespace name",required=true) @PathParam("namespaceName") String namespaceName
,@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema is valid with respect to supplied validation rules" ,required=true) ValidateRequest validateRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.validate(namespaceName,groupName,validateRequest,securityContext);
    }
}

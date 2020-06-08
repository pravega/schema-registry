package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.GroupsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.GroupsApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
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

@Path("/groups")


@io.swagger.annotations.Api(description = "the groups API")

public class GroupsApi  {
   private final GroupsApiService delegate;

   public GroupsApi(@Context ServletConfig servletContext) {
      GroupsApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("GroupsApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (GroupsApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = GroupsApiServiceFactory.getGroupsApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/{groupName}/codecTypes")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new codecType to the group.", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added codecType to group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while registering codectype to a Group", response = Void.class) })
    public Response addCodecType(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "The codecType" ,required=true) String codecType
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addCodecType(groupName,codecType,namespace,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new schema to the group", response = VersionInfo.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 417, message = "Invalid serialization format", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while adding schema to group", response = Void.class) })
    public Response addSchema(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Add new schema to group" ,required=true) SchemaInfo schemaInfo
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchema(groupName,schemaInfo,namespace,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/canRead")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Checks if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = CanRead.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Response to tell whether schema can be used to read existing schemas", response = CanRead.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while checking schema for readability", response = Void.class) })
    public Response canRead(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema can be used to read the data in the stream based on compatibility rules." ,required=true) SchemaInfo schemaInfo
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.canRead(groupName,schemaInfo,namespace,securityContext);
    }
    @POST
    
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createGroup(@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createGroup(createGroupRequest,namespace,securityContext);
    }
    @DELETE
    @Path("/{groupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
    public Response deleteGroup(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteGroup(groupName,namespace,securityContext);
    }
    @DELETE
    @Path("/{groupName}/schemas/{type}/versions/{version}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete schema version from the group.", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Schema corresponding to the version", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting schema from group", response = Void.class) })
    public Response deleteSchemaVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Schema type from SchemaInfo#type or VersionInfo#type",required=true) @PathParam("type") String type
,@ApiParam(value = "Version number",required=true) @PathParam("version") Integer version
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteSchemaVersion(groupName,type,version,namespace,securityContext);
    }
    @DELETE
    @Path("/{groupName}/schemas/versions/{versionOrdinal}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete schema identified by version from the group.", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Schema corresponding to the version", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting schema from group", response = Void.class) })
    public Response deleteSchemaVersionOrdinal(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Version ordinal",required=true) @PathParam("versionOrdinal") Integer versionOrdinal
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteSchemaVersionOrdinal(groupName,versionOrdinal,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/codecTypes")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get codecTypes for the group.", response = CodecTypesList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found CodecTypes", response = CodecTypesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching codecTypes registered", response = Void.class) })
    public Response getCodecTypesList(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getCodecTypesList(groupName,namespace,securityContext);
    }
    @PUT
    @Path("/{groupName}/encodings")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get an encoding id that uniquely identifies a schema version and codec type pair.", response = EncodingId.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name or version not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Codec type not registered", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while getting encoding id", response = Void.class) })
    public Response getEncodingId(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingId(groupName,getEncodingIdRequest,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/encodings/{encodingId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the encoding information corresponding to the encoding id.", response = EncodingInfo.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while getting encoding info corresponding to encoding id", response = Void.class) })
    public Response getEncodingInfo(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Encoding id that identifies a unique combination of schema and codec type",required=true) @PathParam("encodingId") Integer encodingId
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingInfo(groupName,encodingId,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/history")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the history of schema evolution of a Group", response = GroupHistory.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group history", response = GroupHistory.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group history", response = Void.class) })
    public Response getGroupHistory(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupHistory(groupName,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing Group", response = GroupProperties.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = GroupProperties.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupProperties(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupProperties(groupName,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/{type}/versions/{version}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get schema from the version ordinal that uniquely identifies the schema in the group.", response = SchemaInfo.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching schema from version", response = Void.class) })
    public Response getSchemaFromVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Schema type from SchemaInfo#type or VersionInfo#type",required=true) @PathParam("type") String type
,@ApiParam(value = "Version number",required=true) @PathParam("version") Integer version
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersion(groupName,type,version,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions/{versionOrdinal}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get schema from the version ordinal that uniquely identifies the schema in the group.", response = SchemaInfo.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching schema from version", response = Void.class) })
    public Response getSchemaFromVersionOrdinal(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Version ordinal",required=true) @PathParam("versionOrdinal") Integer versionOrdinal
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersionOrdinal(groupName,versionOrdinal,namespace,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/find")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered. It does not automatically register the schema. To add new schema use addSchema", response = VersionInfo.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error fetching version for schema", response = Void.class) })
    public Response getSchemaVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) SchemaInfo schemaInfo
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaVersion(groupName,schemaInfo,namespace,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get all schema versions for the group", response = SchemaVersionsList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaVersionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group schema versions", response = Void.class) })
    public Response getSchemaVersions(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@ApiParam(value = "Type of object the schema describes.") @QueryParam("type") String type
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaVersions(groupName,namespace,type,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch latest schema versions for all objects identified by SchemaInfo#type under a Group. If query param type is specified then latest schema for the type is returned.", response = SchemaVersionsList.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Latest schemas for all objects identified by SchemaInfo#type under the group", response = SchemaVersionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group's latest schemas", response = Void.class) })
    public Response getSchemas(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Type of object") @QueryParam("type") String type
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemas(groupName,type,namespace,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups within the namespace. If namespace is not specified, All groups in default namespace are listed.", response = ListGroupsResponse.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = ListGroupsResponse.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class) })
    public Response listGroups(@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@ApiParam(value = "Continuation token") @QueryParam("continuationToken") String continuationToken
,@ApiParam(value = "The numbers of items to return") @QueryParam("limit") Integer limit
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listGroups(namespace,continuationToken,limit,securityContext);
    }
    @PUT
    @Path("/{groupName}/rules")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Write conflict", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while updating Group's schema validation rules", response = Void.class) })
    public Response updateSchemaValidationRules(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "update group policy" ,required=true) UpdateValidationRulesRequest updateValidationRulesRequest
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateSchemaValidationRules(groupName,updateValidationRulesRequest,namespace,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/validate")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Checks if given schema is compatible with schemas in the registry for current policy setting.", response = Valid.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema validation response", response = Valid.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while trying to validate schema", response = Void.class) })
    public Response validate(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema is valid with respect to supplied validation rules" ,required=true) ValidateRequest validateRequest
,@ApiParam(value = "namespace") @QueryParam("namespace") String namespace
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.validate(groupName,validateRequest,namespace,securityContext);
    }
}

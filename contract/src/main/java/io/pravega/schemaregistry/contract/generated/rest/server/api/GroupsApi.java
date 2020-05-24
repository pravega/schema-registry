package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.GroupsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.GroupsApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
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
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaNamesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
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
    @Path("/{groupName}/codecs")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new codec to the group.", response = Void.class, tags={ "Codecs", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added codec to group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addCodec(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "The codec" ,required=true) AddCodec addCodec
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addCodec(groupName,addCodec,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Adds a new schema to the group", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added schema to the group", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Incompatible schema", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 417, message = "Invalid schema type", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response addSchema(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Add new schema to group" ,required=true) AddSchemaRequest addSchemaRequest
,@ApiParam(value = "Schema Name") @QueryParam("schemaName") String schemaName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addSchema(groupName,addSchemaRequest,schemaName,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/canRead")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Checks if given schema can be used for reads subject to compatibility policy in the schema validation rules.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema can be used to read", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response canRead(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema can be used to read the data in the stream based on compatibility rules." ,required=true) CanReadRequest canReadRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.canRead(groupName,canReadRequest,securityContext);
    }
    @POST
    
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully added group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Group with given name already exists", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a Group", response = Void.class) })
    public Response createGroup(@ApiParam(value = "The Group configuration" ,required=true) CreateGroupRequest createGroupRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createGroup(createGroupRequest,securityContext);
    }
    @DELETE
    @Path("/{groupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the Group", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the Group", response = Void.class) })
    public Response deleteGroup(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteGroup(groupName,securityContext);
    }
    @DELETE
    @Path("/{groupName}/schemas/versions/{versionOrdinal}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete schema version from the group.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Schema corresponding to the version", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response deleteSchemaVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Version ordinal",required=true) @PathParam("version") Integer version
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteSchemaVersion(groupName,version,securityContext);
    }
    @GET
    @Path("/{groupName}/codecs")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get codecs for the group.", response = CodecsList.class, tags={ "Codecs", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Codecs", response = CodecsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getCodecsList(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getCodecsList(groupName,securityContext);
    }
    @PUT
    @Path("/{groupName}/encodings")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get an encoding id that uniquely identifies a schema version and codec pair.", response = EncodingId.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingId.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Codec not registered", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getEncodingId(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetEncodingIdRequest getEncodingIdRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingId(groupName,getEncodingIdRequest,securityContext);
    }
    @GET
    @Path("/{groupName}/encodings/{encodingId}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the encoding information corresponding to the encoding id.", response = EncodingInfo.class, tags={ "Encoding", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Encoding", response = EncodingInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group or encoding id with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getEncodingInfo(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Encoding id that identifies a unique combination of schema and codec",required=true) @PathParam("encodingId") Integer encodingId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getEncodingInfo(groupName,encodingId,securityContext);
    }
    @GET
    @Path("/{groupName}/history")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the history of schema evolution of a Group", response = GroupHistory.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group history", response = GroupHistory.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getGroupHistory(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupHistory(groupName,securityContext);
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
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getGroupProperties(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions/latest")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get latest schema for the group.", response = SchemaWithVersion.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group properties", response = SchemaWithVersion.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getLatestSchema(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Schema Name") @QueryParam("schemaName") String schemaName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLatestSchema(groupName,schemaName,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions/{versionOrdinal}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get schema from the version ordinal that uniquely identifies the schema in the group.", response = SchemaInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema corresponding to the version", response = SchemaInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaFromVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Version ordinal",required=true) @PathParam("version") Integer version
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaFromVersion(groupName,version,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/names")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch all different objects identified by schema names under a Group. This api will return schema types.", response = SchemaNamesList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of objects identified by schema names under the group", response = SchemaNamesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaNames(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaNames(groupName,securityContext);
    }
    @GET
    @Path("/{groupName}/rules")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the schema validation rules configured for the Group", response = SchemaValidationRules.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found Group schema validation rules", response = SchemaValidationRules.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaValidationRules(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaValidationRules(groupName,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/find")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get the version for the schema if it is registered. It does not automatically register the schema. To add new schema use addSchema", response = VersionInfo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = VersionInfo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemaVersion(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Get schema corresponding to the version" ,required=true) GetSchemaVersion getSchemaVersion
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaVersion(groupName,getSchemaVersion,securityContext);
    }
    @GET
    @Path("/{groupName}/schemas/versions")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get all schema versions for the group", response = SchemaVersionsList.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Versioned history of schemas registered under the group", response = SchemaVersionsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response getSchemas(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Schema Name") @QueryParam("schemaName") String schemaName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemas(groupName,schemaName,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all groups", response = ListGroupsResponse.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all groups", response = ListGroupsResponse.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of Groups", response = Void.class) })
    public Response listGroups(@ApiParam(value = "Continuation token") @QueryParam("continuationToken") String continuationToken
,@ApiParam(value = "The numbers of items to return") @QueryParam("limit") Integer limit
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listGroups(continuationToken,limit,securityContext);
    }
    @PUT
    @Path("/{groupName}/rules")
    @Consumes({ "application/json" })
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "update schema validation rules of an existing Group", response = Void.class, tags={ "Group", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Updated schema validation policy", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Write conflict", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response updateSchemaValidationRules(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "update group policy" ,required=true) UpdateValidationRulesRequest updateValidationRulesRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateSchemaValidationRules(groupName,updateValidationRulesRequest,securityContext);
    }
    @POST
    @Path("/{groupName}/schemas/versions/validate")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Checks if given schema is compatible with schemas in the registry for current policy setting.", response = Void.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema is valid", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Group with given name not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Group details", response = Void.class) })
    public Response validate(@ApiParam(value = "Group name",required=true) @PathParam("groupName") String groupName
,@ApiParam(value = "Checks if schema is valid with respect to supplied validation rules" ,required=true) ValidateRequest validateRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.validate(groupName,validateRequest,securityContext);
    }
}

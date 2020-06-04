package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.server.api.SchemasApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.factories.SchemasApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import io.pravega.schemaregistry.contract.generated.rest.model.AddedTo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;

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

@Path("/schemas")


@io.swagger.annotations.Api(description = "the schemas API")

public class SchemasApi  {
   private final SchemasApiService delegate;

   public SchemasApi(@Context ServletConfig servletContext) {
      SchemasApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("SchemasApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (SchemasApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = SchemasApiServiceFactory.getSchemasApi();
      }

      this.delegate = delegate;
   }

    @POST
    @Path("/addedTo")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Gets a map of groups to version info where the schema if it is registered. SchemaInfo#properties is ignored while comparing the schema.", response = AddedTo.class, tags={ "Schema", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Schema version", response = AddedTo.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Schema not found", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching Schema references", response = Void.class) })
    public Response getSchemaReferences(@ApiParam(value = "Get schema references for the supplied schema" ,required=true) SchemaInfo schemaInfo
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getSchemaReferences(schemaInfo,securityContext);
    }
}

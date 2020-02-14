package io.pravega.schemaregistry.server.rest.generated.api;

import io.pravega.schemaregistry.server.rest.generated.api.*;
import io.pravega.schemaregistry.server.rest.generated.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionsList;
import io.pravega.schemaregistry.server.rest.generated.model.CreateGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CreateScopeRequest;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingId;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingInfo;
import io.pravega.schemaregistry.server.rest.generated.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GroupProperty;
import io.pravega.schemaregistry.server.rest.generated.model.GroupsList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaEvolutionList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersion;
import io.pravega.schemaregistry.server.rest.generated.model.ScopesList;
import io.pravega.schemaregistry.server.rest.generated.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.server.rest.generated.model.ValidateRequest;
import io.pravega.schemaregistry.server.rest.generated.model.VersionInfo;

import java.util.List;
import io.pravega.schemaregistry.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class ScopesApiService {
    public abstract Response addSchemaToGroupIfAbsent(String scopeName,String groupName,AddSchemaToGroupRequest addSchemaToGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createGroup(String scopeName,CreateGroupRequest createGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createScope(CreateScopeRequest createScopeRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteGroup(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getCompressionsList(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingInfo(String scopeName,String groupName,Integer encodingId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupProperties(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupSchemas(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestGroupSchema(String scopeName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestSubgroupSchema(String scopeName,String groupName,String subgroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getOrGenerateEncodingId(String scopeName,String groupName,GetEncodingIdRequest getEncodingIdRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersion(String scopeName,String groupName,GetSchemaFromVersionRequest getSchemaFromVersionRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSubGroupSchemas(String scopeName,String groupName,String subgroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listGroups(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listScopes(SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateSchemaValidationRules(String scopeName,String groupName,UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response validate(String scopeName,String groupName,ValidateRequest validateRequest,SecurityContext securityContext) throws NotFoundException;
}

package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

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
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersionModel;
import io.pravega.schemaregistry.contract.generated.rest.model.ScopesListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfoModel;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

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
